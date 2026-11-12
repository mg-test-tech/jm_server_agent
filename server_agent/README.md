# PerfMon Server Agent (OSHI-based)

JMeter PerfMon Server Agent replacement using OSHI instead of Hyperic Sigar.

## Features

- **JDK 17+** — uses modern Java APIs
- **OSHI** — no native libraries (.so/.dll) required
- **Full protocol compatibility** — works with existing PerfMon listener
- **75+ metrics** — CPU, memory, swap, disk I/O, network, TCP, JMX, exec, tail
- **Per-process metrics** — CPU and memory per process
- **TCP + UDP** — both transports supported
- **Custom metrics** — exec commands and file tailing

## Building

```bash
cd server_agent
/opt/gradle/gradle-8.12/bin/gradle shadowJar
```

Output: `build/libs/ServerAgent-3.0.0.jar` (copy to `ServerAgent.jar`)

## Usage

```bash
# Default: TCP and UDP on port 4444
java -jar ServerAgent.jar

# Custom ports
java -jar ServerAgent.jar --tcp-port 5555 --udp-port 0

# Disable exec/tail custom metrics
java -jar ServerAgent.jar --no-exec

# Auto-shutdown after last client disconnects
java -jar ServerAgent.jar --auto-shutdown

# List available system objects
java -jar ServerAgent.jar --sysinfo

# Change metrics polling interval
java -jar ServerAgent.jar --interval 5

# Verbose logging
java -jar ServerAgent.jar --loglevel DEBUG
```

## Protocol

Plain text over TCP/UDP:

```
# Connect via telnet
telnet localhost 4444

# Test connection
test
→ Yep

# Single metric query (TAB-separated)
metrics-single:cpu:combined[TAB]memory:usedperc
→ 22.057369[TAB]57.523596

# Continuous metrics polling
metrics:cpu:combined[TAB]memory:free[TAB]network:rx:iface=eth0
→ Ok
(Agents polls at configured interval)

# Change interval
interval:10
→ Ok

# Disconnect
exit
```

## Supported Metrics

| Category | Types |
|----------|-------|
| CPU | combined, idle, user, system, iowait, irq, nice, softirq, stolen |
| Memory (total) | usedperc, freeperc, used, free, actualfree, actualused, ram, total |
| Memory (process) | resident, virtual, shared, pagefaults, majorfaults, minorfaults |
| Disk I/O | queue, reads, writes, readbytes, writebytes |
| Filesystem | available, free, total, useperc, used, files, freefiles |
|| Network | bytesrecv, bytessent, rx, tx, packetsrecv, packetssent, errors, drops (returns rx\\tx pairs) ||
| TCP | estab, time_wait, close_wait, bound, close, closing, fin_wait1, fin_wait2, idle, inbound, last_ack, listen, outbound, syn_recv |
| Swap | used, pagein, pageout, free, total |
| JMX | gc-time, memory-usage, memory-committed, memorypool-usage, memorypool-committed, class-count, compile-time |
| Custom | exec (shell command), tail (file read) |

## Compatibility

- Works with existing [PerfMon Metrics Collector](https://jmeter-plugins.org/wiki/PerfMon/) listener
- Protocol identical to original Sigar-based ServerAgent
- Same metric types, selectors, and parameter format

## Platform Support

| Platform | OSHI Backend | Notes |
|----------|-------------|-------|
| Linux | procfs, sysfs, commands | No native libs needed |
| Windows | WMI, PDH, Registry | No native libs needed |
| macOS | sysctl, iokit, commands | No native libs needed |
