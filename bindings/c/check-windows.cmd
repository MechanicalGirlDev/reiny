@echo off
setlocal
call "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat"
if errorlevel 1 exit /b 1
cl /nologo /W4 /WX /c /Icrates\reiny-ffi\include bindings\c\smoke.c /Fotarget\debug\c-smoke.obj
if errorlevel 1 exit /b 1
cl /nologo /EHsc /std:c++17 /W4 /WX /c /Icrates\reiny-ffi\include bindings\cpp\smoke.cpp /Fotarget\debug\cpp-smoke.obj
if errorlevel 1 exit /b 1
if not exist target\debug\reiny_ffi.dll.lib (
  echo C and C++ compilation passed; DLL import library not built yet.
  exit /b 0
)
link /nologo target\debug\c-smoke.obj target\debug\reiny_ffi.dll.lib /out:target\debug\c-smoke.exe
if errorlevel 1 exit /b 1
link /nologo target\debug\cpp-smoke.obj target\debug\reiny_ffi.dll.lib /out:target\debug\cpp-smoke.exe
if errorlevel 1 exit /b 1
target\debug\c-smoke.exe
if errorlevel 1 exit /b 1
target\debug\cpp-smoke.exe
exit /b %errorlevel%
