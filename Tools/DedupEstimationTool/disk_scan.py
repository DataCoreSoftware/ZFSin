import config
import fastcdc
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import threading

def process_disk(disk, chunksize, hash_function, m, x, threads, disk_size, pbar, sample_size, lock):
    """
    Creates a threadpool to process the data in the disk.
    """
    logging.debug(f"Starting process_disk for {disk}")
    if sample_size != -1:
        disk_size = sample_size
    
    disk_size -= disk_size % 512
    size_per_thread = (disk_size // threads) + 1
    size_per_thread -= size_per_thread % 512
    
    iters = (size_per_thread // chunksize) + 1
    logging.debug(f"Disk size: {disk_size}, Size per thread: {size_per_thread}, Iterations per thread: {iters}")

    offset = 0
    with ThreadPoolExecutor(max_workers=threads) as executor:
        for i in range(threads):
            if sample_size != -1 and config.bytes_total >= sample_size:
                logging.debug(f"Sample size reached: {config.bytes_total} >= {sample_size}")
                break
            try:
                handle = open(disk, "rb")
                handle.seek(offset, 0)
                
                # Adjust size_per_thread for the last thread
                if i == threads - 1:
                    size_per_thread = disk_size - offset
                
                logging.debug(f"Submitting thread {i} with offset {offset} and size {size_per_thread}")
                executor.submit(
                    process_partial_disk,
                    handle,
                    iters,
                    chunksize,
                    hash_function,
                    m,
                    x,
                    threads,
                    pbar,
                    size_per_thread,
                    lock  # Pass the lock object
                )
                offset += size_per_thread
                config.bytes_total += size_per_thread
            except Exception as e:
                logging.error(f"Error submitting thread {i}: {e}")

def process_partial_disk(handle, iters, chunksize, hash_function, m, x, threads, pbar, size_per_thread, lock):
    """
    Processes a portion of the disk in chunks.
    """
    try:
        logging.debug(f"Thread {threading.current_thread().name} started processing with size {size_per_thread}")
        logging.debug(f"fastcdc.fastcdc initializing now with chunksize {chunksize} and hash_function {hash_function}")
        chunker = fastcdc.fastcdc(handle, chunksize, chunksize, chunksize, hf=hash_function)
        logging.debug(f"Chunker initialized successfully for thread {threading.current_thread().name}")
    except Exception as e:
        clickl.echo(str(e))
        logging.error(f"Error initializing chunker: {e}")
        with lock:
            config.files_skipped += 1  # Increment skipped files count
        return

    iter_count = 0
    for chunk in chunker:
        try:
            logging.debug(f"Processing chunk with hash {chunk.hash}")
            if int(chunk.hash, 16) % m == x:
                with lock:
                    config.fingerprints.add(int(chunk.hash, 16))
                    logging.debug(f"Added chunk hash {chunk.hash} to fingerprints")
        except Exception as e:
            logging.error(f"Error processing chunk: {e}")

        iter_count += 1
        if iter_count >= iters:
            break

    with lock:
        config.bytes_total += iter_count * chunksize
    logging.debug(f"Thread {threading.current_thread().name} processed {iter_count} chunks.")



