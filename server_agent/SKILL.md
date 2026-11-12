# ServerAgent — JDK17 OSHI-based JMeter PerfMon replacement

Use when building, testing, or fixing the ServerAgent (JMeter PerfMon collector backend). Replaces Hyperic Sigar with OSHI 6.6.5, JDK17+.

## Build
```
cd /home/papa/jm_plugins_dev/server_agent/
/opt/gradle/gradle-8.12/bin/gradle shadowJar
cp build/libs/ServerAgent-3.0.0.jar ServerAgent.jar
```

## Launch
```
java -jar ServerAgent.jar --tcp-port 4444 --udp-port 4444
```

## Protocol
- TCP/UDP, colon-separated metric specs, comma OR tab-separated multi-metric
- `test` → `Yep`
- `metrics-single:<metric>` → single numeric value
- `metrics:<metrics>` → tab-separated values (one per metric)
- `interval:<seconds>` → set continuous update interval
- `metrics:<metric>` → continuous updates every interval seconds until `exit`
- `exit` → stop continuous updates (client still connected, can send new metrics)

## Metric syntax
- `cpu:<subtype>` — usedperc, used, combined, idle, user, system, iowait, irq, nice, softirq, stolen
- `memory:<type>` — usedperc, freeperc, used, free, total, available, actualused, actualfree, ram
- `swap:<type>` — used, free, total, usedperc, pagein, pageout
- `network:<alias>` — 27 aliases: rx, tx, rxbytes, txbytes, bytesrecv, bytessent, packetsrecv, packetssent, errors, drops, etc.
- `tcp:<state>` — estab, listen, syn_recv, time_wait, close_wait, close, fin_wait1, fin_wait2, last_ack, bound, idle, closing, outbound, inbound
- `disks:<metric>` — reads, writes, readbytes, writebytes, queue
- `disk:<metric>:<mount>` — used, total, free, useperc, files, freefiles (e.g., `disk:useperc:/`)
- `exec:<command>` — execute shell command, parse numeric output
- `tail:<file>` — read last line of file, parse numeric output
- **All network metrics return SINGLE values in multi-metric mode (original PerfMon compatible)**
- **Standalone `metrics:rx` returns single value (NOT pipe-separated pair)**

## JMeter format
JMeter sends comma-separated metrics: `metrics:cpu:usedperc,memory:usedperc`
Both comma and tab parsing work.

## Key files
- `MetricCollector.java` — core metric collection (OSHI)
- `ProtocolHandler.java` — parsing, routing, network pair formatting
- `MetricsStore.java` — continuous metrics queue management (pipe→tab expansion)
- `TcpServer.java` — TCP server with per-client queues
- `UdpServer.java` — UDP server with direct metric response
- `ArgsParser.java` — command-line argument parsing
- `ClientMetrics.java` — client metrics tracking

## Common issues
- CPU `usedperc` returns NaN — add `usedperc` to switch case alongside `used`
- Network NaN in multi-metric list — rx/tx return single values (not tab-separated pairs) to avoid breaking tab separation
- Swap `usedperc` returns NaN — must calculate `(used/total)*100`
- Memory NaN — add `available` case to switch
- Disk NaN — add `disk:` prefix case (not just `disks:`)
- JMeter comma format not working — ensure `parseMetricsList` splits on comma first
- First poll returns "Error: no connection" — queue not ready yet, second poll works correctly
- Double-scheduler after exit+new metrics — `handleExit` does NOT remove client from map so `handleMetrics` can stop old scheduler; warm-up drains stale queue values and clears `latestResult`

## OSHI API mapping
- CPU: `processor.getSystemCpuLoadTicks()` → combined 8-value array → sum across all
- Memory: `memory.getAvailable()` / `getTotal()` → bytes, convert to percent
- Network: `networkInterface` iteration → sum rx/tx bytes and packets across all
- Disk: `filesystems` iteration → used/free/total from `getUsableSpace()` / `getTotalSpace()`
- TCP: `network.getTcpInfo()` → state-by-state counters
