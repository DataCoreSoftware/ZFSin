import config
import fastcdc
from concurrent.futures import ThreadPoolExecutor
import logging
import sys
import os
import traceback

READ_BLOCK_SIZE = 1024 * 1024           # 1MB read chunks
FASTCDC_WINDOW_SIZE = 16 * 1024 * 1024  # 16MB window for CDC
OVERLAP_SIZE = 2 * 1024 * 1024          # 2MB overlap for chunk boundary safety
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

def process_partial_disk(disk, start_offset, end_offset, chunksize, hash_function, m, x, pbar, sample_size, lock):
    handle = None
    try:
        flags = os.O_RDONLY
        if sys.platform == 'win32':
            flags |= os.O_BINARY

        handle = os.open(disk, flags)
        os.lseek(handle, start_offset, os.SEEK_SET)

        buffer = bytearray()
        processed_bytes = 0
        total_size = end_offset - start_offset

        while processed_bytes < total_size:
            try:
                to_read = min(READ_BLOCK_SIZE, total_size - processed_bytes)
                chunk = os.read(handle, to_read)
                if not chunk:
                    break  # Reached EOF or failed to read
                buffer.extend(chunk)
                processed_bytes += len(chunk)
                pbar.update(len(chunk))
            except Exception as e:
                logging.error(f"Error reading at offset {start_offset + processed_bytes}: {e}")
                logging.error(traceback.format_exc())
                break

            # Process CDC when buffer is large enough
            if len(buffer) >= FASTCDC_WINDOW_SIZE:
                try:
                    window = bytes(buffer[:FASTCDC_WINDOW_SIZE])
                    chunker = fastcdc.fastcdc(window, chunksize, chunksize, chunksize, hf=hash_function)

                    for c in chunker:
                        try:
                            h = int(c.hash, 16)
                            if h % m == x:
                                with lock:
                                    config.fingerprints.add(h)
                        except Exception as e:
                            logging.error(f"Error processing chunk hash: {e}")
                except Exception as e:
                    logging.error(f"FastCDC failed on buffer: {e}")
                    logging.error(traceback.format_exc())

                # Slide buffer forward, keeping overlap
                buffer = buffer[FASTCDC_WINDOW_SIZE - OVERLAP_SIZE:]

        # Process final leftover buffer
        if buffer:
            try:
                chunker = fastcdc.fastcdc(bytes(buffer), chunksize, chunksize, chunksize, hf=hash_function)
                for c in chunker:
                    try:
                        h = int(c.hash, 16)
                        if h % m == x:
                            with lock:
                                config.fingerprints.add(h)
                    except Exception as e:
                        logging.error(f"Error processing final chunk hash: {e}")
            except Exception as e:
                logging.error(f"Final FastCDC failed: {e}")
                logging.error(traceback.format_exc())

        # Stats update
        with lock:
            config.files_scanned += 1
            if sample_size == -1:
                config.bytes_total += processed_bytes
            config.chunk_count += processed_bytes // chunksize
            if processed_bytes % chunksize != 0:
                config.chunk_count += 1
            config.files_skipped += 1

    except Exception as e:
        logging.error(f"Failed to process raw disk {disk}: {repr(e)}")
        logging.error(traceback.format_exc())

    finally:
        if handle is not None:
            os.close(handle)