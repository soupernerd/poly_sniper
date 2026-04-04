@echo off
setlocal
cd /d "%~dp0"

set "PY_CMD="
where python >nul 2>nul
if not errorlevel 1 (
  set "PY_CMD=python"
) else (
  where py >nul 2>nul
  if not errorlevel 1 (
    set "PY_CMD=py -3"
  )
)

if "%PY_CMD%"=="" (
  echo Python is not installed or not on PATH.
  echo Install Python 3.11+ and run this script again.
  pause
  exit /b 1
)

call %PY_CMD% scripts\setup_public.py
if errorlevel 1 (
  echo.
  echo Setup failed.
  pause
  exit /b 1
)

echo.
echo Setup finished.
pause
