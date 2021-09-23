
Steps to run ZTS for windows

3 Additional Steps required for first time

	Step 1 :
		Setup the wsl and package required to run ZTS
		Edit sudo vim /etc/sudoers file to run command without password
		<user>  ALL=(ALL) NOPASSWD:ALL  // This line to add in sudoers file
		Follow this URL for package installation https://openzfs.github.io/openzfs-docs/Developer%20Resources/Building%20ZFS.html

	Step 2 :
		git clone openzfs from https://github.com/DataCoreSoftware/openzfs
		Checkout to zfs-test-suite-win branch
		Build openzfs by follow this URL https://openzfs.github.io/openzfs-docs/Developer%20Resources/Building%20ZFS.html

	Step 3 :
		Edit common.sh file (../openzfs/scripts/common.sh)
		Edit 2 lines
		-> Edit BIN_DIR path
		-> Edit SBIN_DIR path
		***
			export BIN_DIR=/mnt/c/zfs-test-suite/ZFS_Binaries/Release
			export SBIN_DIR=/mnt/c/zfs-test-suite/ZFS_Binaries/Release

		***

Step to run python script

Step 1 : Open cmd with Admin privileges
Seps 2 : Run following to start test
	python.exe ../openzfs/scripts/zts_windows/zts_windows_test.py "<ZFSin Binaries Release.zip file path>"