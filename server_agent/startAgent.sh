#!/bin/bash
# PerfMon Server Agent launcher for Unix/Linux/macOS
# Usage: ./startAgent.sh [--tcp-port PORT] [--udp-port PORT] [--interval SECONDS]
#                        [--no-exec] [--auto-shutdown] [--sysinfo] [--loglevel LEVEL]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AGENT_JAR="${SCRIPT_DIR}/ServerAgent.jar"

if [ ! -f "$AGENT_JAR" ]; then
    echo "Error: ServerAgent.jar not found at $AGENT_JAR"
    echo "Make sure you are in the ServerAgent directory."
    exit 1
fi

exec java -jar "$AGENT_JAR" "$@"
