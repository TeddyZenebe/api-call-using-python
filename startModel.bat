NET USE N: /delete
NET USE C:\Users\tzenebe\Documents\ArcGIS\api-call-using-python


if not "%minimized%"=="" goto :minimized
set minimized=true
@echo off



C:\Python27\ArcGIS10.6\python.exe "C:\Users\tzenebe\Documents\ArcGIS\api-call-using-python/model.py"
pause

goto :EOF
:minimized
exit