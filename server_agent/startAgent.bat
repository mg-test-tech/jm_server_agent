@echo off
REM PerfMon Server Agent launcher for Windows
REM Usage: startAgent.bat [--tcp-port PORT] [--udp-port PORT] [--interval SECONDS]
REM                       [--no-exec] [--auto-shutdown] [--sysinfo] [--loglevel LEVEL]

setlocal
set SCRIPT_DIR=%~dp0
set AGENT_JAR=%SCRIPT_DIR%ServerAgent.jar

if not exist "%AGENT_JAR%" (
    echo Error: ServerAgent.jar not found at %AGENT_JAR%
    echo Make sure you are in the ServerAgent directory.
    exit /b 1
)

java -jar "%AGENT_JAR%" %*
