@echo off
REM HPTL weekly COT update wrapper for Task Scheduler
cd /d "%~dp0.."
set PYTHONPATH=src
set HPTL_SKIP_LIVE_FEEDS=1
python -m hptl.cot.run_update %*
exit /b %ERRORLEVEL%
