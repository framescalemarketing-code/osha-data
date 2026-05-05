@echo off
setlocal

set SCRIPT_DIR=%~dp0
powershell -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%start-dashboard.ps1"
set EXIT_CODE=%ERRORLEVEL%

if not "%EXIT_CODE%"=="0" (
  echo.
  echo Dashboard startup failed with exit code %EXIT_CODE%.
  echo Press any key to close this window.
  pause >nul
)

exit /b %EXIT_CODE%
