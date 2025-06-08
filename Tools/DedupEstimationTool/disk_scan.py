import config
import fastcdc
from concurrent.futures import ThreadPoolExecutor
import logging
import sys
import os
import traceback
from humanize import naturalsize

from collections import defaultdict

READ_BLOCK_SIZE = 1024 * 1024           # 1MB read chunks
FASTCDC_WINDOW_SIZE = 16 * 1024 * 1024  # 16MB window for CDC
OVERLAP_SIZE = 2 * 1024 * 1024          # 2MB overlap for chunk boundary safety

logging.basicConfig(level=logging.DEBUG)

def process_disk(disk, chunksize, hash_function, m, x, threads, disk_size, pbar, sample_size, skip_zeroes, lock):
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
    
    #logging.debug(f"Disk size: {disk_size}, Size per thread: {size_per_thread}")

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
                            skip_zeroes,
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

def process_partial_disk(disk, start_offset, end_offset, chunksize, hash_function, m, x, pbar, skip_zeroes, lock):
    handle = None
    with lock:
        if disk not in config.last_offsets_real:
            config.last_offsets_real[disk] = 0
        if disk not in config.last_offsets_zero:
            config.last_offsets_zero[disk] = 0
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
                #logging.debug(f"Raw read of {to_read} bytes at offset {start_offset + processed_bytes}")
                if not chunk:
                    break
                # If skip-zeroes is on and full buffer is zero, skip ahead
                if skip_zeroes and chunk.count(0) == len(chunk):
                    with lock:
                        config.zero_chunks_skipped += 1
                        config.zero_bytes_skipped += len(chunk)
                        config.last_offsets_zero[disk] = start_offset + processed_bytes + len(chunk)
                        pbar.update(len(chunk))

                    #logging.debug(f"[ZERO SKIP] Offset: {start_offset + processed_bytes}, Size: {len(chunk)}")
                    processed_bytes += len(chunk)
                    continue
                else:
                    buffer.extend(chunk)
                    processed_bytes += len(chunk)
                    with lock:
                        pbar.update(len(chunk))
                    #logging.debug(f"[READ] Offset: {start_offset + processed_bytes - len(chunk)}, Size: {len(chunk)}")
            except Exception as e:
                logging.error(f"Error reading at offset {start_offset + processed_bytes}: {e}")
                logging.error(traceback.format_exc())
                break

            # Process CDC when buffer is large enough
            if len(buffer) >= FASTCDC_WINDOW_SIZE:
                window = bytes(buffer[:FASTCDC_WINDOW_SIZE])
                try:
                    chunker = fastcdc.fastcdc(window, chunksize, chunksize, chunksize, hf=hash_function)

                    for c in chunker:
                        abs_offset = start_offset + processed_bytes - len(buffer) + c.offset
                        h = int(c.hash, 16)
                        if h % m == x:
                            with lock:
                                config.fingerprints.add(h)
                                config.chunk_count += 1
                                if abs_offset < config.last_offsets_real[disk]:
                                    pass  # Overlaps with previous chunk, skip
                                else:
                                    config.bytes_total += c.length
                                    config.last_offsets_real[disk] = abs_offset + c.length
                except Exception as e:
                    logging.error(f"FastCDC failed on buffer: {e}")
                    logging.error(traceback.format_exc())
                finally:
                    # Always trim buffer to maintain progress
                    buffer = buffer[FASTCDC_WINDOW_SIZE - OVERLAP_SIZE:]

        if buffer:  # Process final leftover buffer
            try:
                chunker = fastcdc.fastcdc(bytes(buffer), chunksize, chunksize, chunksize, hf=hash_function)
                for c in chunker:
                    abs_offset = start_offset + total_size - len(buffer) + c.offset
                    h = int(c.hash, 16)
                    if h % m == x:
                            with lock:
                                config.fingerprints.add(h)
                                config.chunk_count += 1
                                if abs_offset < config.last_offsets_real[disk]:
                                    pass  # Overlaps with previous chunk, skip
                                else:
                                    config.bytes_total += c.length
                                    config.last_offsets_real[disk] = abs_offset + c.length
            except Exception as e:
                logging.error(f"Final FastCDC failed: {e}")
                logging.error(traceback.format_exc())

        # Stats update
        with lock:
            config.files_scanned += 1
            '''logging.debug(
                f"[STATS] Disk: {disk} | Bytes Total: {config.bytes_total} | "
                f"Chunks: {config.chunk_count} | Unique: {len(config.fingerprints)} | "
                f"Zero Skipped: {config.zero_chunks_skipped} ({naturalsize(config.zero_bytes_skipped)})"
            )'''

    except Exception as e:
        logging.error(f"Failed to process raw disk {disk}: {repr(e)}")
        logging.error(traceback.format_exc())
        with lock:
            config.files_skipped += 1

    finally:
        #logging.debug(f"Finished processing {disk} from {start_offset} to {end_offset}, now config.bytes_total={config.bytes_total}, config.chunk_count={config.chunk_count}, config.zero_chunks_skipped={config.zero_chunks_skipped}, config.zero_bytes_skipped={config.zero_bytes_skipped}")
        if handle is not None:
            os.close(handle)
