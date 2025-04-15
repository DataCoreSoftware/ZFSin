import config
import fastcdc
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

def process_disk(disk, chunksize, hash_function, m, x, threads, disk_size, pbar, sample_size):
    """
    Creates a threadpool to process the data in the disk.
    """
    if sample_size != -1:
        disk_size = sample_size
    
    disk_size -= disk_size % 512
    size_per_thread = (disk_size // threads) + 1
    size_per_thread -= size_per_thread % 512
    
    iters = (size_per_thread // chunksize) + 1

    offset = 0
    with ThreadPoolExecutor(max_workers=threads) as executor:
        for i in range(threads):
            if sample_size != -1 and config.bytes_total >= sample_size:
                break
            handle = open(disk, "rb")
            handle.seek(offset, 0)
            
            # Adjust size_per_thread for the last thread
            if i == threads - 1:
                size_per_thread = disk_size - offset
            
            logging.debug(f"Submitting thread {i} with offset {offset} and size {size_per_thread}")
            executor.submit(process_partial_disk, handle, iters, chunksize, hash_function, m, x, threads, pbar, size_per_thread)
            offset += size_per_thread
            config.bytes_total += size_per_thread

def process_partial_disk(handle, iters, chunksize, hash_function, m, x, threads, pbar, size_per_thread):
    try:
        chunker = fastcdc.fastcdc(handle, chunksize, chunksize, chunksize, hf=hash_function)
    except Exception as e:
        clickl.echo(str(e))
        with lock:
            config.files_skipped += 1  # Increment skipped files count
        logging.error(f"Error processing disk: {e}")
        return

    k = 256 * 64 // threads
    
    iter_count = 0
    iters_by_k = iters // k
    i = 1
    for chunk in chunker:
        if int(chunk.hash, 16) % m == x:
            # Safely add the chunk hash to the fingerprints
            with lock:
                config.fingerprints.add(int(chunk.hash, 16))
        if iter_count == iters_by_k * i:
            with lock:
                pbar.update(iters_by_k * chunksize)
            i += 1
        if iter_count >= iters:
            break
        iter_count += 1

    # Safely update the total bytes processed
    with lock:
        config.bytes_total += iter_count * chunksize
    logging.debug(f"Thread {threading.current_thread().name} processed {iter_count} chunks.")



