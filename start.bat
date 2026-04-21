@echo off
REM Momentum Screener — Start Script (Windows)
REM Starts the TWS API bridge + opens screener in browser
REM Prerequisites: TWS or IB Gateway must be running and logged in

setlocal enabledelayedexpansion

set SCRIPT_DIR=%~dp0
pushd "%SCRIPT_DIR%"

set PYTHON_EXE=python
if exist "%SCRIPT_DIR%.venv\Scripts\python.exe" set PYTHON_EXE=%SCRIPT_DIR%.venv\Scripts\python.exe
if exist "%SCRIPT_DIR%..\.venv\Scripts\python.exe" set PYTHON_EXE=%SCRIPT_DIR%..\.venv\Scripts\python.exe

set FORCE_TERMINAL=
if exist "%SCRIPT_DIR%.local-dev-terminal" set FORCE_TERMINAL=1
if exist "%SCRIPT_DIR%..\.local-dev-terminal" set FORCE_TERMINAL=1

set SERVE_PORT=8888
set TWS_PORT=
set COGNITO_POOL=
set COGNITO_CLIENT=

REM Parse optional args
:parse_args
if "%~1"=="" goto done_args
if "%~1"=="--tws-port" (set TWS_PORT=%~2& shift & shift & goto parse_args)
if "%~1"=="--port" (set SERVE_PORT=%~2& shift & shift & goto parse_args)
if "%~1"=="--live" (set TWS_PORT=7496& shift & goto parse_args)

if "%~1"=="--cognito-pool" (set COGNITO_POOL=%~2& shift & shift & goto parse_args)
if "%~1"=="--cognito-client" (set COGNITO_CLIENT=%~2& shift & shift & goto parse_args)
shift
goto parse_args
:done_args

REM ── Check Python dependencies ──────────────────────────────
if exist "%~dp0requirements.txt" (
    "%PYTHON_EXE%" -m pip install -q -r "%~dp0requirements.txt"
) else (
    "%PYTHON_EXE%" -c "import ib_insync, aiohttp, yfinance, tzdata" 2>nul
    if errorlevel 1 (
        echo Installing Python dependencies...
        "%PYTHON_EXE%" -m pip install ib_insync aiohttp yfinance pywebview tzdata
    )
)

REM ── Kill old processes on our port ─────────────────────────
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":%SERVE_PORT% " ^| findstr "LISTENING"') do (
    taskkill /F /PID %%a >nul 2>&1
)
REM Also kill any leftover pywebview launcher (fixed port 18710)
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":18710 " ^| findstr "LISTENING"') do (
    taskkill /F /PID %%a >nul 2>&1
)
timeout /t 1 /nobreak >nul

REM ── Check if TWS/Gateway is reachable ──────────────────────
if "%TWS_PORT%"=="" (
    echo TWS port: auto-detect ^(will try 7497, 7496^)
) else (
    echo Checking for TWS/IB Gateway on port %TWS_PORT%...
    powershell -Command "try { $c = New-Object Net.Sockets.TcpClient('127.0.0.1', %TWS_PORT%); $c.Close(); exit 0 } catch { exit 1 }" >nul 2>&1
    if errorlevel 1 (
        echo.
        echo   TWS/Gateway not found on port %TWS_PORT% -- starting anyway.
        echo   The bridge will keep retrying until TWS is connected.
        echo.
    ) else (
        echo TWS/Gateway found on port %TWS_PORT%
    )
)

REM ── Launch native window ────────────────────────────────────
echo Starting BlitzTrade...
echo Using Python: %PYTHON_EXE%

REM Set Cognito env vars before any if/else block (batch %% expansion gotcha)
if not "%COGNITO_POOL%"=="" (
    set COGNITO_USER_POOL_ID=%COGNITO_POOL%
    set COGNITO_CLIENT_ID=%COGNITO_CLIENT%
)

set USE_BROWSER_MODE=
if "%FORCE_TERMINAL%"=="1" set USE_BROWSER_MODE=1
if not "%FORCE_TERMINAL%"=="1" (
    "%PYTHON_EXE%" -c "import webview" >nul 2>&1
    if errorlevel 1 set USE_BROWSER_MODE=1
)

if not "%USE_BROWSER_MODE%"=="1" (
    if not "%TWS_PORT%"=="" set TWS_PORT=%TWS_PORT%
    "%PYTHON_EXE%" "%SCRIPT_DIR%launcher.py"
) else (
    if "%FORCE_TERMINAL%"=="1" (
        echo   Local dev terminal mode enabled via .local-dev-terminal
        echo.
    ) else (
        echo   pywebview not found -- falling back to browser mode
        echo   Install it:  pip install pywebview
        echo.
    )
    echo ============================================
    echo   Screener: http://localhost:%SERVE_PORT%
    if "%TWS_PORT%"=="" (echo   TWS API:  auto-detect) else (echo   TWS API:  127.0.0.1:%TWS_PORT%)
    echo ============================================
    echo.

    start http://localhost:%SERVE_PORT%

    echo Running. Press Ctrl+C to stop.
    if "%TWS_PORT%"=="" (
        "%PYTHON_EXE%" "%SCRIPT_DIR%serve.py" %SERVE_PORT%
    ) else (
        "%PYTHON_EXE%" "%SCRIPT_DIR%serve.py" %SERVE_PORT% --tws-port %TWS_PORT%
    )
    echo.
    echo BlitzTrade stopped.
)

popd
