import config
import fastcdc
from concurrent.futures import ThreadPoolExecutor
import logging
import threading
from io import BytesIO

#logging.basicConfig(level=logging.DEBUG)

def process_disk(disk, chunksize, hash_function, m, x, threads, disk_size, pbar, sample_size, lock):
    """
    Orchestrates the parallel processing of a disk by dividing it based on the number of threads
    and assigning each part to a thread for processing.
    """
    #logging.debug(f"Starting process_disk for {disk}")
    #print("Starting process_disk for", disk)

    # Adjust disk size based on sample_size
    if sample_size != -1:
        disk_size = min(disk_size, sample_size)
    disk_size -= disk_size % chunksize  # Ensure disk_size is a multiple of chunksize

    # Calculate the size of each thread's workload
    size_per_thread = (disk_size + threads - 1) // threads
    size_per_thread -= size_per_thread % chunksize  # Ensure size_per_thread is a multiple of chunksize
    iters = size_per_thread // chunksize  # Calculate iterations per thread

    #logging.debug(f"Disk size: {disk_size}, Size per thread: {size_per_thread}, Iterations per thread: {iters}")

    # Open the file handle
    try:
        with open(disk, "rb") as handle:
            #logging.debug(f"Opened raw disk {disk} successfully")
            # Use ThreadPoolExecutor to process parts in parallel
            with ThreadPoolExecutor(max_workers=threads) as executor:
                futures = []
                for i in range(threads):
                    start_offset = i * size_per_thread
                    end_offset = min((i + 1) * size_per_thread, disk_size)

                    if start_offset < disk_size:
                        #logging.debug(f"Submitting thread {i} with start_offset {start_offset} and end_offset {end_offset}")
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
    except Exception as e:
        logging.error(f"Failed to open raw disk {disk}: {e}")
        return

def process_partial_disk(disk, start_offset, end_offset, chunksize, hash_function, m, x, pbar, sample_size, lock):
    """
    Processes a portion of the disk assigned to a thread. It reads the disk in chunks, computes a hash for each chunk,
    and performs deduplication or filtering based on the hash.
    """
    
    
    with open(disk, "rb") as handle:
        #logging.debug(f"Thread {threading.current_thread().name} started processing from offset {start_offset} to {end_offset}")
        handle.seek(start_offset)
        processed = 0  # Bytes processed


        # Processing loop
        while processed < (end_offset - start_offset):
            part_disk_size = end_offset - start_offset
            part_disk = handle.read(part_disk_size)
            if not part_disk:
                break  # EOF or partial read

            try:
                chunker = fastcdc.fastcdc(part_disk, chunksize, chunksize, chunksize, hf=hash_function)

                for chunk in chunker:
                    if int(chunk.hash, 16) % m == x:
                        # The chunk passes the filter. So add it to the fingerprints
                        with lock:
                            config.fingerprints.add(int(chunk.hash, 16))
            except Exception as e:
                logging.error(f"Error processing chunk: {e}")

            # Update the progress bar and increment processed bytes
            with lock:
                config.files_scanned += 1
                if sample_size == -1:
                    config.bytes_total += part_disk_size

                config.chunk_count += part_disk_size // chunksize
                if part_disk_size % chunksize != 0:
                    config.chunk_count += 1
                #pbar.update(len(chunk))

            processed += len(part_disk)
            iter_count += 1
            pbar.update(len(part_disk))

        with lock:
            config.files_skipped += 1


