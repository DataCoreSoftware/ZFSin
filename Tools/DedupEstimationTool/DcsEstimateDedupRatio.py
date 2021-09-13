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
    # global files_skipped
    if recursive:
        for root, subdirs, files in os.walk(path):
            for name in files:
                filepath = os.path.join(root, name)
                try:
                    size = os.path.getsize(filepath)
                    yield [filepath, size]
                except:
                    config.files_skipped += 1
    else:
        for name in os.listdir(path):
            filepath = os.path.join(path, name)
            if os.path.isfile(filepath):
                try:
                    size = os.path.getsize(filepath)
                    yield [filepath, size]
                except:
                    config.files_skipped += 1


@click.command(cls=DefaultHelp)
@click.argument(
    "paths",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=False, resolve_path=True), nargs=-1,
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

#@click.option(
#    "-dt",
#    "--disk-type",
#    help="ld: logical disk, pd: physical disk. Applicable if you want to scan raw disk",
#    type=click.Choice(['ld', 'pd'], case_sensitive=False)
#)


@click.option(
    "-raw",
    help="To scan raw disk",
    is_flag=True,
    default=False,
)

@click.option(
    "--nosampling",
    help="To disable sampling",
    is_flag=True,
    default=False,
)

def scan(paths, recursive, size, hash_function, outpath, max_threads, raw):
    """
    Scan and report duplication.
    """
    click.echo("DCS Deduplication Estimation Tool\n")
    
   if nosampling:
        m = 1
        x = 0
    else:
        m = 1000    # 1 in m chunks are included in the sample
        x = random.randint(0, m - 1)    # random integer between 0 and m.

    '''
    m and x together act as a filter and decide whether a chunk will be stored in the sample
    '''
    
    supported = supported_hashes()
    hf = getattr(hashlib, hash_function)

    path_count = len(paths) # number of input path
    bytes_total = 0


    # if disk_type:
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
                bytes_total += dsize
            except:
                import wmi
                # if disk_type == 'pd':
                for d in wmi.WMI().Win32_DiskDrive():
                        if d.DeviceID == path:
                            dsize = int(d.size)
                            sizes.append(dsize)
                            bytes_total += dsize
                # if disk_type == 'ld':
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

        i = 0
        bar_format = '{l_bar}{bar}| [time elapsed: {elapsed}, time remaining: {remaining}]'
        with tqdm(total=bytes_total, desc = "Estimating dedup ratio", unit= " path", ascii=' #', bar_format=bar_format, leave=False) as pbar:
            # seperately process each disk
            with ThreadPoolExecutor(max_workers=path_count) as executor:
                for path in paths:
                    executor.submit(process_disk, path, size, hf, m, x, max_threads, sizes[i], pbar)
                    i += 1

        t.stop()
        
        #global fingerprints
        bytes_unique = min(len(config.fingerprints) * m * size, bytes_total)
        
        if bytes_total:
            if bytes_unique:
                results = {}
                results['paths'] = list(paths)
                time_taken = int(Timer.timers.mean("scan"))
                data_per_s = bytes_total / time_taken
                click.echo("Unique Chunks:\t{}".format(intcomma(len(config.fingerprints))))
                results["unique_chunks"] = intcomma(len(config.fingerprints))
                click.echo("Unique Data:\t{}".format(naturalsize(bytes_unique, True)))
                results["unique_data"] = naturalsize(bytes_unique, True)
                click.echo("Total Data:\t{}".format(naturalsize(bytes_total, True)))
                results["total_data"] = naturalsize(bytes_total, True)
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
                print("Very less unique data / High Duplication. Or try running the tool as administrator")
        else:
            click.echo("No data.\n")

    else:
        '''
        For files
        '''

        click.echo("Scanning the paths...")
        
        file_count = 0 # total number of files
        for path in paths:
            for file in iter_files(path, recursive):
                bytes_total += file[1]
                file_count += 1
                click.echo("\rNumber of files found: {}".format(intcomma(file_count)), nl=False)

        click.echo("\nTotal size: {}\n".format(naturalsize(bytes_total, True)))
        
        t = Timer("scan", logger=None)
        t.start()

        bar_format = '{l_bar}{bar}| {n_fmt}/{total_fmt} [time elapsed: {elapsed}, time remaining: {remaining}]'
        with tqdm(total=file_count, desc = "Estimating dedup ratio", unit= " file", ascii=' #', bar_format=bar_format, leave=False) as pbar:
            # seperately process files in each input path
            with ThreadPoolExecutor(max_workers=path_count) as executor:
                for path in paths:
                    executor.submit(process_path, path, recursive, size, hf, m, x, max_threads, pbar)

        t.stop()
        
        #global fingerprints
        bytes_unique = min(len(config.fingerprints) * m * size, bytes_total)
        
        if bytes_total:
            if bytes_unique:
                results = {}
                results['paths'] = list(paths)
                time_taken = int(Timer.timers.mean("scan"))
                data_per_s = bytes_total / time_taken
                results["files"] = intcomma(file_count)
                click.echo("Unique Chunks:\t{}".format(intcomma(len(config.fingerprints))))
                results["unique_chunks"] = intcomma(len(config.fingerprints))
                click.echo("Unique Data:\t{}".format(naturalsize(bytes_unique, True)))
                results["unique_data"] = naturalsize(bytes_unique, True)
                click.echo("Total Data:\t{}".format(naturalsize(bytes_total, True)))
                results["total_data"] = naturalsize(bytes_total, True)
                click.echo("DeDupe Ratio:\t{:.2f}".format(bytes_total / bytes_unique))
                results["dedup_ratio"] = round(bytes_total / bytes_unique, 2)
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
                print("Very less unique data / High Duplication. Or try running the tool as administrator")
        else:
            click.echo("No data.\n")


if __name__ == "__main__":
    scan()


