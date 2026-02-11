@echo off
setlocal
pythonw "%~dp0stooq_incremental_updater.py" --gui
if errorlevel 1 exit /b %errorlevel%
