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

import fastcdc
from fastcdc.utils import DefaultHelp, supported_hashes


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
    type=click.Path(exists=True, file_okay=True, dir_okay=True, readable=False, resolve_path=True), nargs=-1,
    #type=click.Path(exists=True, readable=False, resolve_path=True), nargs=-1,
    required = True
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
    default="xxh32",
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
    "-raw",
    help="To scan raw disk",
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

def scan(paths, recursive, size, hash_function, outpath, max_threads, raw, nosampling, sample_size, isconfig):
    """
    Scan and report duplication.
    """
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

    '''
    m and x together act as a filter and decide whether a chunk will be stored in the sample
    '''

    if sample_size != -1:
        sample_size = sample_size  * 1024 * 1024 * 1024
    
    supported = supported_hashes()
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
                            
        if not sizes or not bytes_total:
            click.echo("Wrong path or incorrect type.")
            return

        click.echo("Total size: {}\n".format(naturalsize(bytes_total, True)))
            
        t = Timer("scan", logger=None)
        t.start()

        if sample_size != -1:
            bytes_total = sample_size

        i = 0
        bar_format = '{l_bar}{bar}| [time elapsed: {elapsed}, time remaining: {remaining}]'
        with tqdm(total=bytes_total, desc = "Estimating dedup ratio", unit= " path", ascii=' #', bar_format=bar_format, leave=False) as pbar:
            # seperately process each disk
            with ThreadPoolExecutor(max_workers=path_count) as executor:
                for path in paths:
                    executor.submit(process_disk, path, size, hf, m, x, max_threads, sizes[i], pbar, sample_size)
                    i += 1

        t.stop()

        if sample_size != -1:
            bytes_total = config.bytes_total
        
        bytes_unique = min(len(config.fingerprints) * m * size, bytes_total)
        
        if bytes_total:
            if bytes_unique:
                results = {}
                results['paths'] = list(paths)
                time_taken = int(Timer.timers.mean("scan")) + 0.1
                data_per_s = bytes_total / time_taken
                click.echo("Unique Chunks:\t{}".format(intcomma(len(config.fingerprints))))
                results["unique_chunks"] = intcomma(len(config.fingerprints))
                click.echo("Unique Data:\t{}".format(naturalsize(bytes_unique, True)))
                results["unique_data"] = naturalsize(bytes_unique, True)
                click.echo("Data scanned:\t{}".format(naturalsize(bytes_total, True)))
                results["data_scanned"] = naturalsize(bytes_total, True)
                click.echo("DeDupe Ratio:\t{:.2f}".format(bytes_total / bytes_unique))
                results["dedup_ratio"] = round(bytes_total / bytes_unique, 2)
                click.echo("Throughput:\t{}/s".format(naturalsize(data_per_s, True)))
                results["throughput"] = str(naturalsize(data_per_s, True)) + "/s"
                click.echo("\nTime taken:\t{}".format(str(precisedelta(datetime.timedelta(seconds=time_taken)))))
                
                # Saving the output in a json file.
                output = "DedupEstimationResult.txt"
                with open(os.path.join(outpath,output), "w") as outfile:
                    outfile.write(json.dumps(results, indent = 2))
            else:
                print("Very less unique data / High Duplication.\nUse --nosampling option.\nOr try running the tool as administrator")
        else:
            click.echo("No data.\n")

    else:
        '''
        For files
        '''

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

        bar_format = '{l_bar}{bar}| {n_fmt}/{total_fmt} [time elapsed: {elapsed}, time remaining: {remaining}]'
        with tqdm(total=files_to_scan, desc = "Estimating dedup ratio", unit= " file", ascii=' #', bar_format=bar_format, leave=False) as pbar:
            # seperately process files in each input path
            with ThreadPoolExecutor(max_workers=path_count) as executor:
                for path in paths:
                    executor.submit(process_path, path, recursive, size, hf, m, x, max_threads, pbar, sample_size)

        t.stop()

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
                click.echo("Unique Chunks:\t{}".format(intcomma(chunks_unique)))
                results["unique_chunks"] = intcomma(chunks_unique)
                click.echo("Unique Data:\t{}".format(naturalsize(bytes_unique, True)))
                results["unique_data"] = naturalsize(bytes_unique, True)
                click.echo("Data scanned:\t{}".format(naturalsize(bytes_total, True)))
                results["data_scanned"] = naturalsize(bytes_total, True)
                click.echo("DeDupe Ratio:\t{:.2f}".format(dedup_ratio))
                results["dedup_ratio"] = round(dedup_ratio, 2)
                click.echo("Throughput:\t{}/s".format(naturalsize(data_per_s, True)))
                results["throughput"] = str(naturalsize(data_per_s, True)) + "/s"
                click.echo("Files Skipped:\t{}".format(intcomma(config.files_skipped)))
                results["files_skippped"] = config.files_skipped
                click.echo("\nTime taken:\t{}".format(str(precisedelta(datetime.timedelta(seconds=time_taken)))))
                
                # Saving the output in a json file.
                output = "DedupEstimationResult.txt"
                with open(os.path.join(outpath,output), "w") as outfile:
                    outfile.write(json.dumps(results, indent = 2))
            else:
                click.echo("Very less unique data / High Duplication.\nUse --nosampling option.\nOr try running the tool as administrator if admin privileges are required to read the files")
            if sample_size != -1:
                click.echo("\nIf the file sizes exceed the sample size, the data scanned will be less than the sample size. To scan more data, increase the sample size")
        else:
            click.echo("No data or cannot access the files.\n")


if __name__ == "__main__":
    scan()


