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
                except:
                    pass
    else:
        for name in os.listdir(path):
            filepath = os.path.join(path, name)
            if os.path.isfile(filepath):
                try:
                    size = os.path.getsize(filepath)
                    yield [filepath, size]
                except:
                    pass


def process_path(path, recursive, chunksize, hash_function, m, x, threads, pbar):
    """
    creates a threadpool to process the files in given path
    """
    def handler(fut):
        pbar.update(1)
        
    with ThreadPoolExecutor(max_workers=threads) as executor:
        for file in iter_files(path, recursive):
            executor.submit(process_file, file[0], chunksize, hash_function, m, x).add_done_callback(handler)
            
    
def process_file(file, chunksize, hash_function, m, x):
    """
    Process the file and add the hash of unique chunks of the file into the global fingerprints.
    """
    #global files_skipped
    try:
        chunker = fastcdc.fastcdc(file, chunksize, chunksize, chunksize, hf = hash_function)
    except Exception as e:
        config.files_skipped += 1
        return

    #global fingeprints
    
    for chunk in chunker:
        if int(chunk.hash, 16) % m == x:
            # the chunk passes the filter. So add it to the fingerprints
            config.fingerprints.add(int(chunk.hash, 16))


