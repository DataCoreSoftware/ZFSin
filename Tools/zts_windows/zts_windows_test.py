# -*- coding: utf-8 -*-
"""
Created on Fri Apr  9 17:20:30 2021

@author: Administrator
"""
import sys
import os
from zts_util_func import *

def normal_run():
    print("================= Normal run ================= \n")
    if install_zfsin():
        print("============== Installation Successfully Done============= \n")
    else :
        print("================ Error while installing ================= \n")
        return True

    if runtests():
        print("============== Tests run Successfully ================= \n")
    else :
        print("=================== Something went wrong ===================\n")
        return True

    return False

def first_run():
    print("============= First run ================= \n")
    return

def cleanup():
    print("====================== Cleanup running ===================== \n")
    save_history()

    if uninstall_zfsin():
        print("=================== Uninstalled Successfully Done ===================== \n")
    else :
        print("===================== Error while uninstalling ==================== \n")
        return

    remove_zfsin()
    print("===================== Cleanup done =======================")
    return

def main(argv):

    if not os.path.exists("C:/zfs-test-suite") :
        make_dir()
        cp_runfile_wsl()

    set_config_intiall(argv)

    if not clean_setup():
        reboot()
        return

    clean_disk()

    if normal_run() :
            return

    cleanup()

    set_config_val('first run','start','false')
    reboot()
    return

if __name__ == "__main__":
    main(sys.argv)
