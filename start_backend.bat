@echo off
pushd "%~dp0"

REM Attiva la venv
call ".venv\Scripts\activate.bat"

REM Avvia il backend
python run.py

pause
