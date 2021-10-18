import config
import fastcdc
from concurrent.futures import ThreadPoolExecutor, as_completed


def process_disk(disk, chunksize, hash_function, m, x, threads, disk_size, pbar, sample_size):
    """
    creates a threadpool to process the data in the disk
    """
    disk_size -= disk_size % 512
    size_per_thread = (disk_size // threads) + 1
    size_per_thread -= size_per_thread % 512

    if sample_size != -1 and sample_size <= size_per_thread:
        size_per_thread = sample_size
        size_per_thread -= size_per_thread % 512
    
    iters = (size_per_thread // chunksize) + 1

    offset = 0
    with ThreadPoolExecutor(max_workers=threads) as executor:
        for i in range(threads):
            if sample_size != -1 and config.bytes_total >= sample_size:
                break
            handle = open(disk, "rb")
            handle.seek(offset,0)
            executor.submit(process_partial_disk, handle, iters, chunksize, hash_function, m, x, threads, pbar)
            offset += size_per_thread
            config.bytes_total += size_per_thread
             

def process_partial_disk(handle, iters, chunksize, hash_function, m, x, threads, pbar):
    try:
        chunker = fastcdc.fastcdc(handle, chunksize, chunksize, chunksize, hf = hash_function)
    except Exception as e:
        clickl.echo(str(e))
        return

    k = 256 * 64 // threads
    
    iter_count = 0
    iters_by_k = iters // k
    i = 1
    for chunk in chunker:
        if int(chunk.hash, 16) % m == x:
            # the chunk passes the filter. So add it to the fingerprints
            config.fingerprints.add(int(chunk.hash, 16))
        if iter_count == iters_by_k * i:
            pbar.update(iters_by_k * chunksize)
            i += 1
        if iter_count >= iters:
            break
        iter_count += 1



