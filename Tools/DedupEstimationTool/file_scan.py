import config
import os
import fastcdc
from concurrent.futures import ThreadPoolExecutor, as_completed


def iter_files(path, recursive=False):
    if recursive:
        for root, subdirs, files in os.walk(path):
            for name in files:
                filepath = os.path.join(root, name)
                try:
                    size = os.path.getsize(filepath)
                    yield [filepath, size]
                except GeneratorExit:
                    return
                except:
                    config.files_skipped += 1
    else:
        for name in os.listdir(path):
            filepath = os.path.join(path, name)
            if os.path.isfile(filepath):
                try:
                    size = os.path.getsize(filepath)
                    yield [filepath, size]
                except GeneratorExit:
                    return
                except:
                    config.files_skipped += 1


def process_path(path, recursive, chunksize, hash_function, m, x, threads, pbar, sample_size):
    """
    creates a threadpool to process the files in given path
    """
    def handler(fut):
        pbar.update(1)

    if sample_size != -1:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            for file in iter_files(path, recursive):
                if config.bytes_total + file[1] > sample_size:
                    continue
                try:
                    f = open(file[0], 'r')
                    f.close()
                    executor.submit(process_file, file[0], file[1], chunksize, hash_function, m, x, sample_size).add_done_callback(handler)
                    config.bytes_total += file[1]
                except:
                    config.files_skipped += 1
    else:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            for file in iter_files(path, recursive):
                executor.submit(process_file, file[0], file[1], chunksize, hash_function, m, x, sample_size).add_done_callback(handler)


def process_file(file, filesize, chunksize, hash_function, m, x, sample_size):
    """
    Process the file and add the hash of unique chunks of the file into the global fingerprints.
    """
    try:
        chunker = fastcdc.fastcdc(file, chunksize, chunksize, chunksize, hf = hash_function)
    except Exception as e:
        config.files_skipped += 1
        return
    
    for chunk in chunker:
        if int(chunk.hash, 16) % m == x:
            # the chunk passes the filter. So add it to the fingerprints
            config.fingerprints.add(int(chunk.hash, 16))

    config.files_scanned += 1
    if sample_size == -1:
        config.bytes_total += filesize

    config.chunk_count += filesize // chunksize
    if filesize % chunksize != 0:
        config.chunk_count += 1
        
    
