@echo off
title PolySnipe HFT [STANDALONE]
cd /d "%~dp0"
echo.
echo  *** HFT STANDALONE RUNTIME ***
echo  Dashboard: http://127.0.0.1:8898
echo.
if not exist ".venv\Scripts\python.exe" (
  echo Missing local venv at app\.venv
  echo Run from repo root: setup-public.bat
  echo Legacy/manual only: app\setup-venv.bat
  pause
  exit /b 1
)
".venv\Scripts\python.exe" main.py
set EXIT_CODE=%ERRORLEVEL%
if "%EXIT_CODE%"=="0" (
  exit /b 0
)
echo.
echo Runtime exited with code %EXIT_CODE%.
pause
exit /b %EXIT_CODE%
