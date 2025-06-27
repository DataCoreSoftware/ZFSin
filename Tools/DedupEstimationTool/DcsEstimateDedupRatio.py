import config
config.init()
from file_scan import process_path
from disk_scan import process_disk
import hashlib
import random
import os
import json
import datetime
import click
from tqdm import tqdm
from humanize import intcomma, naturalsize, precisedelta
from codetiming import Timer
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import logging

logging.basicConfig(level=logging.DEBUG)

from fastcdc.utils import DefaultHelp, supported_hashes

# Create a lock for thread-safe access to shared resources
lock = threading.Lock()

import queue
import time

progress_queue = queue.Queue()
stop_event = threading.Event()

def progress_updater(pbar, q, stop_event, interval=1.0):
    import time
    import queue

    last_time = time.time()
    pending = 0
    latest_postfix = {}
    last_postfix = {}

    while not stop_event.is_set() or not q.empty():
        try:
            while True:
                delta, postfix = q.get_nowait()
                pending += delta
                if postfix:
                    latest_postfix.update(postfix)
        except queue.Empty:
            pass

        now = time.time()
        if now - last_time >= interval or stop_event.is_set():
            if pending > 0:
                pbar.update(pending)
                if latest_postfix != last_postfix:
                    pbar.set_postfix(latest_postfix, refresh=True)
                    last_postfix = latest_postfix.copy()
                pending = 0
            last_time = now

        time.sleep(0.1)

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

@click.command(cls=DefaultHelp)
@click.argument(
    "paths",
    type=click.Path(exists=True, file_okay=True, dir_okay=True, readable=False), nargs=-1,
    required=True
)
@click.option(
    "-r", "--recursive/--non-recursive",
    help="Scan directory tree recursively",
    default=True,
    show_default=True
)
@click.option(
    "-s",
    "--size",
    type=click.INT,
    default=131072,
    help="The size of the chunks in bytes",
    show_default=True,
)
@click.option(
    "-hf",
    "--hash-function",
    type=click.STRING,
    default="sha256",
    show_default=True
)
@click.option(
    "-o",
    "--outpath",
    help="Location to save the output.",
    type=click.Path(exists=True, file_okay=False, resolve_path=True),
    default=os.getcwd()
)
@click.option(
    "-t",
    "--max-threads",
    help="Number of threads to use.",
    type=click.INT,
    default=16,
    show_default=True
)
@click.option(
    "--raw",
    help="To scan raw disk",
    is_flag=True,
    default=False,
)
@click.option(
    "--skip-zeroes",
    help="Skip all zero chunks when scanning raw disks",
    is_flag=True,
    default=False,
)
@click.option(
    "--nosampling",
    help="Use this option to get more accurate results. But it uses more memory",
    is_flag=True,
    default=False
)
@click.option(
    "--sample-size",
    help="Size of the sample to be scanned (in GB)",
    default=-1
)
@click.option(
    "--isconfig",
    help="Given path is a path to config file",
    is_flag=True,
    default=False,
    show_default=True
)
@click.option(
    "--suppress-result",
    help="Use this option to suppress final result on screen. Useful for running from a script. But this will not hide the progress bar.",
    is_flag=True,
    default=False
)
def scan(paths, recursive, size, hash_function, outpath, max_threads, raw, skip_zeroes, nosampling, sample_size, isconfig, suppress_result):
    """
    Scan and report duplication.
    """
    try:
        click.echo("DCS Deduplication Estimation Tool\n")
        
        if isconfig:
            config_file = open(paths[0])
            paths = config_file.read().split('\n')

        if nosampling:
            m = 1
            x = 0
        else:
            m = 1000    # 1 in m chunks are included in the sample
            x = random.randint(0, m - 1)    # random integer between 0 and m.

        #logging.debug(f"Sampling parameters: m={m}, x={x}")

        '''
        m and x together act as a filter and decide whether a chunk will be stored in the sample
        '''
        if sample_size != -1:
            sample_size = sample_size * 1024 * 1024 * 1024

        hf = getattr(hashlib, hash_function)

        path_count = len(paths) # number of input path
        bytes_total = 0

        if raw:
            '''
            For reading raw physical/logical disks
            '''
            click.echo("Scanning the disks...")
            sizes = []
            for path in paths:
                try:
                    fd = os.open(path, os.O_RDONLY)
                    dsize = os.lseek(fd, 0, os.SEEK_END)
                    sizes.append(dsize)
                    bytes_total += dsize
                except:
                    import wmi
                    for d in wmi.WMI().Win32_DiskDrive():
                        if d.DeviceID == path:
                            dsize = int(d.size)
                            sizes.append(dsize)
                            bytes_total += dsize
                    for d in wmi.WMI().Win32_LogicalDisk():
                        if d.name == path[4:]:
                            dsize = int(d.size)
                            sizes.append(dsize)
                            bytes_total += dsize
                #logging.debug(f"Raw disk path: {path}")
                #logging.debug(f"Disk size: {sizes}")

            if not sizes or not bytes_total:
                click.echo("Wrong path or incorrect type.")
                return

            click.echo("Total size: {}\n".format(naturalsize(bytes_total, True)))

            t = Timer("scan", logger=None)
            t.start()

            if sample_size != -1:
                bytes_total = sample_size

            #logging.debug(f"Starting disk scan now... calling ThreadPoolExecutor with max_workers={path_count}")

            i = 0
            bar_format = '{l_bar}{bar}| [time elapsed: {elapsed}, time remaining: {remaining}{postfix}]'
            with tqdm(total=bytes_total, desc="Estimating dedup ratio", unit="B", ascii=' #', bar_format=bar_format, leave=False) as pbar:
                updater_thread = threading.Thread(target=progress_updater, args=(pbar, progress_queue, stop_event))
                updater_thread.start()

                with ThreadPoolExecutor(max_workers=path_count) as executor:
                    for path in paths:
                        executor.submit(process_disk, path, size, hf, m, x, max_threads, sizes[i], progress_queue, sample_size, skip_zeroes, lock)
                        i += 1

                stop_event.set()
                updater_thread.join()

            t.stop()

            with lock:
                if sample_size != -1:
                    bytes_total = config.bytes_total
                    

            #logging.debug(f"Unique chunks: {len(config.fingerprints)}")
            unique_chunks = set(config.fingerprints)
            bytes_unique = min(len(unique_chunks) * m * size, bytes_total)
            zero_bytes_skipped = config.zero_bytes_skipped
            bytes_actual_total = bytes_total - zero_bytes_skipped

            if bytes_total:
                if bytes_unique:
                    results = {}
                    results['paths'] = list(paths)
                    time_taken = int(Timer.timers.mean("scan")) + 0.1
                    data_per_s = bytes_total / time_taken
                    results["unique_chunks"] = intcomma(len(unique_chunks))
                    results["unique_data"] = naturalsize(bytes_unique, True)
                    results["non_zero_data_allocated"] = naturalsize(bytes_actual_total, True)
                    results["space_scanned"] = naturalsize(bytes_total, True)
                    results["dedup_ratio"] = round(bytes_actual_total / bytes_unique, 2)
                    results["throughput"] = str(naturalsize(data_per_s, True)) + "/s"
                    
                    # Saving the output in a json file.
                    output = "DedupEstimationResult.txt"
                    with open(os.path.join(outpath,output), "w") as outfile:
                        outfile.write(json.dumps(results, indent = 2))

                    if not suppress_result:
                        click.echo("Unique Chunks:\t{}".format(intcomma(len(unique_chunks))))
                        click.echo("Unique Data:\t{}".format(naturalsize(bytes_unique, True)))
                        click.echo("Non-Zero Data Allocated:\t{}".format(naturalsize(bytes_actual_total, True)))
                        click.echo("Space scanned:\t{}".format(naturalsize(bytes_total, True)))
                        click.echo("DeDupe Ratio:\t{:.2f}".format(bytes_actual_total / bytes_unique))
                        click.echo("Throughput:\t{}/s".format(naturalsize(data_per_s, True)))
                        click.echo("\nTime taken:\t{}".format(str(precisedelta(datetime.timedelta(seconds=time_taken)))))
                else:
                    print("Too few unique chunks were detected. This could be due to: (1) high duplication, (2) insufficient sample size, (3) skipped zeroes, or (4) insufficient permissions.\nUse --nosampling option.\nOr skip --skip-zeroes option.\nOr try running the tool as administrator")
            else:
                click.echo("No data.\n")

        else:
            click.echo("Scanning the paths...")
            file_count = 0 # total number of files
            files_to_scan = 0
            bytes_sample = 0

            for path in paths:
                for file in iter_files(path, recursive):
                    if sample_size != -1 and bytes_sample + file[1] <= sample_size:
                        try:
                            f = open(file[0], 'r')
                            f.close()
                            files_to_scan += 1
                            bytes_sample += file[1]
                        except:
                            pass
                    with lock:
                        bytes_total += file[1]
                        file_count += 1
                    click.echo("\rNumber of files found: {}".format(intcomma(file_count)), nl=False)

            click.echo("\nTotal size: {}".format(naturalsize(bytes_total, True)))
            if sample_size == -1:
                files_to_scan = file_count
            else:
                click.echo("Sample size: {}\n".format(naturalsize(sample_size, True)))

            t = Timer("scan", logger=None)
            t.start()

            bar_format = '{l_bar}{bar}| {n_fmt}/{total_fmt} [time elapsed: {elapsed}, time remaining: {remaining}{postfix}]'
            with tqdm(total=files_to_scan, desc="Estimating dedup ratio", unit=" file", ascii=' #', bar_format=bar_format, leave=False) as pbar:
                # seperately process files in each input path
                updater_thread = threading.Thread(target=progress_updater, args=(pbar, progress_queue, stop_event))
                updater_thread.start()
                with ThreadPoolExecutor(max_workers=path_count) as executor:
                    for path in paths:
                        executor.submit(process_path, path, recursive, size, hf, m, x, max_threads, pbar, sample_size, progress_queue)

            t.stop()

            with lock:
                click.echo("\nFiles scanned: {}".format(str(config.files_scanned)))
                bytes_total = config.bytes_total
                chunks_unique = min(len(config.fingerprints) * m, config.chunk_count)
                dedup_ratio = config.chunk_count / chunks_unique
                bytes_unique = int(bytes_total / dedup_ratio)

            if bytes_total:
                if bytes_unique:
                    results = {}
                    results['paths'] = list(paths)
                    time_taken = int(Timer.timers.mean("scan")) + 0.1
                    data_per_s = bytes_total / time_taken
                    results["files"] = intcomma(file_count)
                    results["unique_chunks"] = intcomma(chunks_unique)
                    results["unique_data"] = naturalsize(bytes_unique, True)
                    results["non_zero_data_allocated"] = naturalsize(bytes_total, True)
                    results["space_scanned"] = naturalsize(bytes_total, True)
                    results["dedup_ratio"] = round(dedup_ratio, 2)
                    results["throughput"] = str(naturalsize(data_per_s, True)) + "/s"
                    results["files_skippped"] = config.files_skipped

                    output = "DedupEstimationResult.txt"
                    with open(os.path.join(outpath, output), "w") as outfile:
                        outfile.write(json.dumps(results, indent=2))

                    if not suppress_result:
                        click.echo("Unique Chunks:\t{}".format(intcomma(chunks_unique)))
                        click.echo("Unique Data:\t{}".format(naturalsize(bytes_unique, True)))
                        click.echo("Non-Zero Data Allocated:\t{}".format(naturalsize(bytes_total, True)))
                        click.echo("Space scanned:\t{}".format(naturalsize(bytes_total, True)))
                        click.echo("DeDupe Ratio:\t{:.2f}".format(dedup_ratio))
                        click.echo("Throughput:\t{}/s".format(naturalsize(data_per_s, True)))
                        click.echo("Files Skipped:\t{}".format(intcomma(config.files_skipped)))
                        click.echo("\nTime taken:\t{}".format(str(precisedelta(datetime.timedelta(seconds=time_taken)))))
                else:
                    click.echo("Very less unique data / High Duplication.\nUse --nosampling option.\nOr try running the tool as administrator if admin privileges are required to read the files")
                if sample_size != -1:
                    click.echo("\nIf the file sizes exceed the sample size, the space scanned will be less than the sample size. To scan more data, increase the sample size")
            else:
                click.echo("No data or cannot access the files.\n")
        return 0
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return 2


if __name__ == "__main__":
    scan()


