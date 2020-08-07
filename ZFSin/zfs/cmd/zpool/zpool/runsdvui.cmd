cd /d "C:\zfs\ZFSin\zfs\cmd\zpool\zpool" &msbuild "zpool.vcxproj" /t:sdvViewer /p:configuration="Debug" /p:platform="x64" /p:SolutionDir="C:\zfs" 
exit %errorlevel% 