# -*- coding: utf-8 -*-
"""
Util Functions

"""
import subprocess
from configparser import ConfigParser
import os
import zipfile
import shutil
import datetime
import _thread
import time


config_path = "C:/zfs-test-suite/ZFS_Test_Config.ini"


def unzip_file():
    print("========================= Unzipping zip folder ========================= \n")
    with zipfile.ZipFile(zip_file_path,"r") as zip_ref:
        zip_ref.extractall("C:\\zfs-test-suite\\ZFS_Binaries")

    return


def clean_disk():
    print("======================= Cleaning Disk ======================\n")
    process = createSubprocess('diskpart.exe')
    disk = config_read("pool_disk","disk_id")
    print(disk)
    disk_arr = disk.split(',')
    for disk_no in disk_arr :
        process.stdin.write("select disk "+disk_no + "\n")
        process.stdin.write("clean" + "\n")
        process.stdin.write("convert gpt" + "\n")

    process.stdin.close()
    output = process.stdout.read()
    print(output)
    print("===================== Cleaning disk successfully ================== \n")
    return

def install_zfsin():
    unzip_file()
    print("================= Creating Thread for Popup window ==============\n ")
    try:
        _thread.start_new_thread( check_popup, () )
    except:
        print ("Error: unable to start thread")

    print("====================== Installing ZFSin driver ======================\n ")

    process = createSubprocess('powershell.exe')
    process.stdin.write('cd /d c:\\' + "\n")
    process.stdin.write("cd \"c:/zfs-test-suite/ZFS_Binaries/Release\"" + "\n")
    process.stdin.write(".\zfsinstaller install .\ZFSin\ZFSin.inf" + "\n")
    process.stdin.close()
    output = process.stdout.read()
    print(output)
    if zfsin_status():
        return True

    set_config_val('error','error','true')
    set_config_val('error','error_msg','Error in Installing')
    return False

def remove_zfsin():
    print("================ Removing Release Binaries and Release.zip folder =============\n")
    process = createSubprocess('powershell.exe')

    process.stdin.write("wsl rm -rf /mnt/c/zfs-test-suite/ZFS_Binaries/Release"+"\n")
    process.stdin.write("del "+zip_file_path+"\n")
    process.stdin.close()
    output = process.stdout.read()
    print(output)


def zfsin_status():
    print("=================== CHecking ZFSin status ================== \n")
    process = createSubprocess('cmd.exe')

    process.stdin.write('cd /d c:\\' + "\n")
    process.stdin.write("cd \"c:/zfs-test-suite/ZFS_Binaries/Release\"" + "\n")
    process.stdin.write("zpool export -a" + "\n")
    process.stdin.write("zpool list" + "\n")
    process.stdin.close()
    output = process.stdout.read().split("\n")

    line = False
    for str in output:
        if line:
            if "no pools" in str:
                return True
            else :
                return False
        if "zpool list" in str:
            line = True
    return False

def check_popup():
    status = True
    count = 0
    while status:
        time.sleep(20)
        count = count + 1
        process = createSubprocess('powershell.exe')
        process.stdin.write('cd /d c:\\' + "\n")
        process.stdin.write("cd \"c:/zfs-test-suite\"" + "\n")
        process.stdin.write('.\sendkeys.bat "Windows Security" "{UP}{DOWN}{ENTER}"'+'\n')
        process.stdin.write('.\sendkeys.bat "Error" "{ENTER}"'+'\n')
        process.stdin.close()
        if zfsin_status() or count > 10:
            status = False
            return
    return

def uninstall_zfsin():

    if zfsin_status():
        print("=================== Uninstalling ZFSin driver ================ \n")

        process = createSubprocess('powershell.exe')
        process.stdin.write('cd /d c:\\' + "\n")
        process.stdin.write("cd \"c:/zfs-test-suite/ZFS_Binaries/Release\"" + "\n")
        process.stdin.write(".\zfsinstaller uninstall .\ZFSin\ZFSin.inf" + "\n")
        process.stdin.close()
        output = process.stdout.read()
        print (output)
        return True

    set_config_val('error','error','true')
    set_config_val('error','error_msg','Dataset present')

    return False

def save_history():
    print("============== Saving current result to history ================ \n")
    count = datetime.datetime.now().strftime("%d-%m-%Y_%H:%M")
    process = createSubprocess('powershell.exe')

    folder = "Result_"+count
    process.stdin.write("wsl cp -r /mnt/c/zfs-test-suite/Results /mnt/c/zfs-test-suite/History/"+folder+ "\n")
    process.stdin.close()
    output = process.stdout.read()
    set_run_status()
    print(output)
    return

def set_run_status():
    print("================ Update current run status ================= \n")
    f = open("C:\\zfs-test-suite\\check_status.txt","w+")

    if config_read('last run','run_percentage') == '100':
        print("============ Marking Build as Success =============\n")
        f.write("0")
    else :
        print("============ Marking Build as Failure ============== \n")
        f.write("1")

    f.close()
    return

def runtests():
    print("************************ Tests start running ********************* \n")
    process = createSubprocess('powershell.exe')

    process.stdin.write("wsl /etc/test-suite-run.sh 2>&1 | tee C:/zfs-test-suite/Results/tests_results.txt"+"\n")
    process.stdin.close()
    output = process.stdout.read()
    print(output)
    copy_logs()
    return True

def copy_logs():
    process = createSubprocess('powershell.exe')
    process.stdin.write("wsl cp /var/tmp/test_results/current/log /mnt/c/zfs-test-suite/Results/test_logs.txt"+"\n")
    process.stdin.close()
    output = process.stdout.read()
    print(output)

    total_run = ""
    total_run = run_percentage()
    print("================== Pass Percentage : "+total_run+" =============")
    total_run = total_run.split('.')[0]
    set_config_val('last run','run_percentage',total_run)
    return

def run_percentage():
    print("=================== checking Tests pass percentage ============== \n")
    filePath = "C:\\zfs-test-suite\\Results\\tests_results.txt"
    with open(filePath,"r",encoding = 'utf16') as f:
        Lines = f.readlines()

    for line in Lines:
        if "Percent passed" in line:
            arr = line.strip().split(' ')
            return arr[-1].split(':')[-1].strip()

    return "NA"

def config_read(section,key_list):
    '''
    This method read config file perticular section.
    Arguments (str, str): section,key_list
    Return (str): output
    '''

    parser = ConfigParser()
    parser.read(config_path)
    output = ''
    for sect in parser.sections():
        if sect.strip() == section:
            for key,val in parser.items(sect):
                if key.strip() == key_list:
                    output = val
                    return output

    return ""

def set_config_val(section, key , val):
    configur = ConfigParser()
    configur.read(config_path)
    configur.set(section, key , val)
    with open(config_path, 'w') as configfile:
        configur.write(configfile)

    return

def make_dir():
    process = createSubprocess('cmd.exe')

    process.stdin.write('cd /d c:\\' + "\n")
    process.stdin.write("cd "+script_path()+"\n")
    process.stdin.write("mkdir C:\\zfs-test-suite\\"+"\n")
    process.stdin.write("copy ZFS_Test_Config.ini C:\\zfs-test-suite"+"\n")
    process.stdin.write("copy sendkeys.bat C:\\zfs-test-suite"+"\n")
    process.stdin.write("mkdir C:\\zfs-test-suite\\Results\\"+"\n")
    process.stdin.write("mkdir C:\\zfs-test-suite\\History\\"+"\n")
    process.stdin.write("mkdir C:\\zfs-test-suite\\ZFS_Binaries\\"+"\n")
    process.stdin.close()
    output = process.stdout.read()
    print(output)
    return

def export_disk():
    disk = config_read("pool_disk","disk_id")
    print(disk)
    disk_no = disk.split(',')
    disk_path =''
    for i in disk_no:
        disk_path = disk_path+"PHYSICALDRIVE"+i+" "
    disk_path = '"'+disk_path.strip()+'"'
    print(disk_path)
    return disk_path

def cp_to_linux():
    print("=================== copy to linux ============== \n")
    process = createSubprocess('powershell.exe')
    process.stdin.write("wsl sudo cp /mnt/c/zfs-test-suite/testing.txt /etc/test-suite-run.sh"+"\n")
    process.stdin.write("wsl sudo chmod +x /etc/test-suite-run.sh"+"\n")
    process.stdin.write("wsl sudo apt install dos2unix"+"\n")
    process.stdin.write("wsl sudo dos2unix /etc/test-suite-run.sh"+"\n")
    process.stdin.close()
    output = process.stdout.read()
    print(output)
    return

def cp_runfile_wsl():
    print("=================== Making run file ============== \n")

    filePath = "C:\\zfs-test-suite\\testing.txt"
    openzfs_folder = openzfs_path_linux()
    line1 = "#/bin/bash"
    line2 = 'export DISKS='+export_disk()
    line3 = openzfs_folder + "scripts/zfs-tests.sh"+" -r "+ openzfs_folder +"tests/runfiles/windows.run"
    print(line1)
    print(line2)
    print(line3)
    with open(filePath,"w+") as f:
        f.write(line1+'\n'+'\n')
        f.write(line2+'\n')
        f.write(line3+'\n')

    cp_to_linux()
    return

def set_config_intiall(argv):
    print("=================== setting intial values ============== \n")
    global zip_file_path
    zip_file_path = argv[1]
    set_config_val('error','error','false')
    set_config_val('error','error_msg','NA')
    set_config_val('last run','run_percentage','0')
    set_config_val('first run','start','true')
    set_config_val('last run','run_percentage','NA')

def clean_setup():
    print("=================== Cleaning setup ============== \n")
    set_run_status()
    process = createSubprocess('powershell.exe')
    if os.path.exists("C:/zfs-test-suite/ZFS_Binaries/Release"):
        if uninstall_zfsin():
            print("========== Marking Build failure due to previous Binaries installed  and done uninstallation ========= \n")
            return False
        else :
            process.stdin.write("wsl rm -rf /mnt/c/zfs-test-suite/ZFS_Binaries/Release"+"\n")
    process.stdin.write("del C:/zfs-test-suite/Results/test_logs.txt"+"\n")
    process.stdin.write("del C:/zfs-test-suite/Results/tests_results.txt"+"\n")
    process.stdin.close()
    output = process.stdout.read()
    print(output)
    return True

def script_path():
    cpath = os.path.dirname(__file__)
    return cpath

def openzfs_path_linux():
    cpath = script_path()
    openzfspath = "/mnt/c/"
    arr = cpath.split('\\')
    print(arr)

    for x in range(1,len(arr)-2):
        print(arr[x])
        openzfspath = openzfspath + arr[x]+'/'
    print(openzfspath)
    return openzfspath

def openzfs_path_windows():
    cpath = script_path()
    arr = cpath.split('\\')
    openzfspath = arr[0]
    for x in range(1,len(arr)-2):
        print(arr[x])
        openzfspath = openzfspath+"/"+ arr[x]
    print(openzfspath)

def reboot():
    print("================== System Reboot calling============== \n")
    process = createSubprocess('cmd.exe')
    process.stdin.write("shutdown /r" + "\n")
    process.stdin.close()
    output = process.stdout.read()
    print(output)
    print("============= Shutdown system in next 1 min ================= \n")
    return

def createSubprocess(processType) :
    process = subprocess.Popen(processType, stdin=subprocess.PIPE,
    stdout=subprocess.PIPE, stderr=subprocess.PIPE,encoding = 'utf8',
    universal_newlines=True, bufsize=0,
    creationflags=subprocess.CREATE_NEW_CONSOLE,shell=False)
    return process

'''
def dowload_tools():
    print("========================= Download essential packages ======================== \n")
    process = subprocess.Popen('powershell.exe', stdin=subprocess.PIPE,
    stdout=subprocess.PIPE, stderr=subprocess.PIPE,encoding = 'utf8',
    universal_newlines=True, bufsize=0,
    creationflags=subprocess.CREATE_NEW_CONSOLE,shell=False)
    process.stdin.write("wsl sudo apt-get update"+"\n")
    process.stdin.write("wsl sudo apt-get -y upgrade"+"\n")
    process.stdin.write("wsl sudo apt install -y dh-autoreconf"+"\n")
    process.stdin.write("wsl sudo apt install -y autoconf"+"\n")
    process.stdin.write("wsl sudo apt install build-essential"+"\n")
    process.stdin.write("wsl sudo apt-get install -y libtool"+"\n")
    process.stdin.write("wsl sudo apt install zlib1g-dev"+"\n")
    process.stdin.write("wsl sudo apt install -y uuid-dev"+"\n")
    process.stdin.write("wsl sudo apt install -y libblkid-dev"+"\n")
    process.stdin.write("wsl sudo apt install -y libssl-dev"+"\n")
    process.stdin.write("wsl sudo apt-get install -y linux-generic"+"\n")
    process.stdin.write("wsl sudo apt-get install -y alien"+"\n")
    process.stdin.write("wsl sudo apt install -y python3-dev python3-setuptools python3-cffi"+"\n")
    process.stdin.write("wsl sudo apt install -y ksh"+"\n")
    process.stdin.write("wsl sudo apt-get install -y fio"+"\n")

    process.stdin.close()
    output = process.stdout.read()
    print(output+"\n")
    return


def build_openzfs():
    print("======================= Building openZFS ========================== \n")
    process = subprocess.Popen('powershell.exe', stdin=subprocess.PIPE,
    stdout=subprocess.PIPE, stderr=subprocess.PIPE,encoding = 'utf8',
    universal_newlines=True, bufsize=0,
    creationflags=subprocess.CREATE_NEW_CONSOLE,shell=False)

    process.stdin.write('cd /d c:\\' + "\n")
    process.stdin.write("cd \"C:/zfs-test-suite/openzfs\"" + "\n")
    process.stdin.write("wsl sh autogen.sh"+"\n")
    process.stdin.write("wsl sudo ./configure"+"\n")
    process.stdin.write("wsl sudo make -j8"+"\n")

    process.stdin.close()
    output = process.stdout.read()
    print (output)

'''