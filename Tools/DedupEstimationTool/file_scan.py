import config
import os
import fastcdc
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Create a lock for thread-safe access to shared resources
lock = threading.Lock()

# Configure logging
logging.basicConfig(level=logging.DEBUG)

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
                    with lock:
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
                    with lock:
                        config.files_skipped += 1


def process_path(path, recursive, chunksize, hash_function, m, x, threads, pbar, sample_size):
    """
    Creates a threadpool to process the files in the given path.
    """
    def handler(fut):
        with lock:
            pbar.update(1)

    if sample_size != -1:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            for file in iter_files(path, recursive):
                with lock:
                    if config.bytes_total + file[1] > sample_size:
                        continue
                    config.bytes_total += file[1]
                try:
                    f = open(file[0], 'r')
                    f.close()
                    executor.submit(process_file, file[0], file[1], chunksize, hash_function, m, x, sample_size, pbar).add_done_callback(handler)
                except Exception as e:
                    with lock:
                        config.files_skipped += 1
                    logging.error(f"Error opening file {file[0]}: {e}")
    else:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            for file in iter_files(path, recursive):
                executor.submit(process_file, file[0], file[1], chunksize, hash_function, m, x, sample_size, pbar).add_done_callback(handler)


def process_file(file, filesize, chunksize, hash_function, m, x, sample_size, pbar):
    """
    Process the file and add the hash of unique chunks of the file into the global fingerprints.
    """
    if filesize == 0:
        with lock:
            config.files_skipped += 1
        logging.warning(f"Skipping empty file {file}")
        return
    try:
        chunker = fastcdc.fastcdc(file, chunksize, chunksize, chunksize, hf=hash_function)
    except Exception as e:
        with lock:
            config.files_skipped += 1
        logging.error(f"Error processing file {file}: {e}")
        return
    
    for chunk in chunker:
        h = int(chunk.hash, 16)
        if h % m == x:
            # The chunk passes the filter. So add it to the fingerprints
            with lock:
                config.fingerprints.add(h)

    with lock:
        config.files_scanned += 1
        if sample_size == -1:
            config.bytes_total += filesize

        config.chunk_count += filesize // chunksize
        if filesize % chunksize != 0:
            config.chunk_count += 1


