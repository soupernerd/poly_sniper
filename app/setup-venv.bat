@echo off
cd /d "%~dp0"
echo Creating standalone HFT venv...
python -m venv .venv
if errorlevel 1 (
  echo Failed to create venv.
  pause
  exit /b 1
)
echo Installing requirements...
".venv\Scripts\python.exe" -m pip install --upgrade pip
".venv\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 (
  echo Failed to install requirements.
  pause
  exit /b 1
)
echo Done. Use start-hft.bat to launch.
pause

