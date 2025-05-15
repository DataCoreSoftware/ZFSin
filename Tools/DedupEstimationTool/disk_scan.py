import config
import fastcdc
from concurrent.futures import ThreadPoolExecutor
import logging
import sys
import os

READ_BLOCK_SIZE = 1024 * 1024  # 1 MiB per read
#logging.basicConfig(level=logging.DEBUG)

def process_disk(disk, chunksize, hash_function, m, x, threads, disk_size, pbar, sample_size, lock):
    """
    Orchestrates the parallel processing of a disk by dividing it based on the number of threads
    and assigning each part to a thread for processing.
    """
    logging.debug(f"Starting process_disk for {disk}")
    #print("Starting process_disk for", disk)

    # Adjust disk size based on sample_size
    if sample_size != -1:
        disk_size = min(disk_size, sample_size)
    disk_size -= disk_size % chunksize  # Ensure disk_size is a multiple of chunksize

    # Calculate the size of each thread's workload
    size_per_thread = (disk_size + threads - 1) // threads
    size_per_thread -= size_per_thread % chunksize  # Ensure size_per_thread is a multiple of chunksize
    
    logging.debug(f"Disk size: {disk_size}, Size per thread: {size_per_thread}")

    # Use ThreadPoolExecutor to process parts in parallel
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = []
        for i in range(threads):
            start_offset = i * size_per_thread
            end_offset = min((i + 1) * size_per_thread, disk_size)

            if start_offset < disk_size:
                logging.debug(f"Submitting thread {i} with start_offset {start_offset} and end_offset {end_offset}")
                try:
                    futures.append(
                        executor.submit(
                            process_partial_disk,
                            disk,
                            start_offset,
                            end_offset,
                            chunksize,
                            hash_function,
                            m,
                            x,
                            pbar,
                            sample_size,
                            lock
                        )
                    )
                except Exception as e:
                    logging.error(f"Error submitting thread {i}: {e}")

        # Wait for all threads to complete
        for future in futures:
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error in thread execution: {e}")

def read_large_range(handle, start_offset, total_bytes):
    os.lseek(handle, start_offset, os.SEEK_SET)

    data = bytearray()
    bytes_left = total_bytes

    while bytes_left > 0:
        read_size = min(READ_BLOCK_SIZE, bytes_left)
        chunk = os.read(handle, read_size)
        if not chunk:
            break
        data.extend(chunk)
        bytes_left -= len(chunk)

    return bytes(data)


def process_partial_disk(disk, start_offset, end_offset, chunksize, hash_function, m, x, pbar, sample_size, lock):
    handle = None
    try:
        flags = os.O_RDONLY
        if sys.platform == 'win32':
            flags |= os.O_BINARY

        handle = os.open(disk, flags)

        # Read disk region in safe blocks
        total_size = end_offset - start_offset
        part_data = read_large_range(handle, start_offset, total_size)

        # Apply FastCDC
        try:
            chunker = fastcdc.fastcdc(part_data, chunksize, chunksize, chunksize, hf=hash_function)

            for chunk in chunker:
                if int(chunk.hash, 16) % m == x:
                    with lock:
                        config.fingerprints.add(int(chunk.hash, 16))

        except Exception as e:
            logging.error(f"Error processing chunk: {e}")

        # Update stats
        with lock:
            config.files_scanned += 1
            if sample_size == -1:
                config.bytes_total += len(part_data)

            config.chunk_count += len(part_data) // chunksize
            if len(part_data) % chunksize != 0:
                config.chunk_count += 1

        pbar.update(len(part_data))

        with lock:
            config.files_skipped += 1

    except Exception as e:
        logging.error(f"Failed to process raw disk {disk}: {e}")

    finally:
        if handle is not None:
            os.close(handle)

