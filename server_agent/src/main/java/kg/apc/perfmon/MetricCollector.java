package kg.apc.perfmon;

import oshi.SystemInfo;
import oshi.hardware.*;
import oshi.software.os.FileSystem;
import oshi.software.os.InternetProtocolStats;
import oshi.software.os.InternetProtocolStats.IPConnection;
import oshi.software.os.InternetProtocolStats.TcpState;
import oshi.software.os.InternetProtocolStats.TcpStats;
import oshi.software.os.OSFileStore;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.management.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Collects system metrics using OSHI.
 * Replaces Hyperic Sigar for the PerfMon Server Agent.
 *
 * Implements the same metric types and selector syntax as the original agent:
 *   cpu, memory, swap, disks, network, tcp, exec, tail, jmx
 */
public class MetricCollector {

    private static final Logger log = LogManager.getLogger(MetricCollector.class);

    private final SystemInfo systemInfo = new SystemInfo();
    private final HardwareAbstractionLayer hardware;
    private final OperatingSystem operatingSystem;
    private final MemoryMXBean memoryMXBean;
    private final List<GarbageCollectorMXBean> gcBeans;
    private final ClassLoadingMXBean classLoadingBean;
    private final CompilationMXBean compilationBean;
    private final OperatingSystemMXBean osMXBean;
    private final List<MemoryPoolMXBean> memoryPoolBeans;
    private final boolean noExec;

    // Rate calculation: stores previous counter values for delta computation
    private final Map<String, Long> prevNetRx = new ConcurrentHashMap<>();
    private final Map<String, Long> prevNetTx = new ConcurrentHashMap<>();
    private final Map<String, Long> prevNetPktRx = new ConcurrentHashMap<>();
    private final Map<String, Long> prevNetPktTx = new ConcurrentHashMap<>();
    private final Map<String, Long> prevNetErr = new ConcurrentHashMap<>();
    private final Map<String, Long> prevNetDrop = new ConcurrentHashMap<>();
    private final Map<String, Long> prevNetOutDrop = new ConcurrentHashMap<>();
    // Disk counters are keyed by "diskName|op" (op: reads|writes|readbytes|writebytes|queue)
    // so that the warm-up poll and every scheduled poll share one baseline.
    private final Map<String, Long> prevDiskCounters = new ConcurrentHashMap<>();
    private final Map<String, Long> prevCpuTicks = new ConcurrentHashMap<>();
    private final Map<String, Double> prevCpuUsage = new ConcurrentHashMap<>();
    private final Map<String, OSProcess> prevProcessSnapshots = new ConcurrentHashMap<>();
    private long lastPollTime = 0;

    /**
     * Disk I/O metric spec formats (JMeter PerfMon compatible):
     *   disks i/o:reads:fs=/path     (type "disks i/o", subtype "reads", selector "fs=/path")
     *   diskio:readbytes             (bare counter over all disks)
     *   disks:fs=/path:writes        (fs= in subtype position, op in selector position)
     * The fs= selector may appear in either the subtype or the selector position.
     *
     * @return parsed [fsPath, operation] or null if the spec is not a disk I/O metric
     */
    private String[] parseDiskSpec(String metric) {
        String[] parts = metric.split(":", 3);
        // JMeter sends "disks i/o" which normalizes to "disksio" (space and slash removed).
        // Also accept "disks", "disk", "diskio" for robustness.
        String type = parts[0].trim().toLowerCase().replace(" ", "").replace("/", "");
        boolean isDisk = type.equals("disks") || type.equals("disk") || type.equals("diskio")
            || type.equals("disksio");
        if (!isDisk) return null;
        String a = parts.length > 1 ? parts[1].trim() : "";
        String b = parts.length > 2 ? parts[2].trim() : "";
        String fsPath = null, operation = null;
        if (a.startsWith("fs=")) {
            fsPath = a.substring(3);
            operation = b;
        } else if (b.startsWith("fs=")) {
            fsPath = b.substring(3);
            operation = a;
        } else {
            operation = a;
        }
        return new String[]{fsPath, operation != null ? operation.toLowerCase() : ""};
    }

    /**
     * Collect a disk I/O metric as a delta since the previous poll (0.0 on the
     * first poll, which establishes the baseline). JMeter's
     * generate2Samples() expects monotonically increasing counters and computes
     * the displayed rate client-side, so the agent returns raw counters here and
     * deltas after the baseline poll — same contract as network rx/tx.
     *
     * @param metricSpec full disk metric spec, e.g. "disks i/o:reads:fs=/home"
     * @return delta value, or NaN if no disk matched the fs= selector
     */
    public double collectDiskIo(String metricSpec) {
        String[] parsed = parseDiskSpec(metricSpec);
        if (parsed == null) return Double.NaN;
        String fsPath = parsed[0];
        String operation = parsed[1];

        // Non-rate (gauge) disk metrics: return the current value directly.
        switch (operation) {
            case "total":
            case "used":
            case "free":
            case "available":
            case "useperc":
            case "usedperc":
            case "files":
            case "freefiles":
                // fsPath may be null (no fs= selector) — pass "" so collectFilesystem
                // treats it as "all stores" instead of hitting a null selector.
                return collectFilesystem(operation, fsPath != null ? fsPath : "");
            case "":
                return Double.NaN;
        }

        boolean isOp = operation.equals("reads") || operation.equals("read")
            || operation.equals("writes") || operation.equals("write")
            || operation.equals("readbytes") || operation.equals("writebytes")
            || operation.equals("queue");
        if (!isOp) return Double.NaN;

        // Rate metrics sum over ALL physical disks (same pattern as network I/O:
        // sum over all real interfaces). The fs= mount-point selector is ignored
        // for rate metrics — mount matching is unreliable (a mount like /home may
        // not be a separate partition) and JMeter's disk I/O display is per-disk,
        // not per-mount. This is what makes disk I/O work the way network I/O works.
        List<HWDiskStore> disks = hardware.getDiskStores();

        long total = 0;
        for (HWDiskStore disk : disks) {
            long value;
            switch (operation) {
                case "reads":
                case "read":
                    value = disk.getReads();
                    break;
                case "writes":
                case "write":
                    value = disk.getWrites();
                    break;
                case "readbytes":
                    value = disk.getReadBytes();
                    break;
                case "writebytes":
                    value = disk.getWriteBytes();
                    break;
                default: // queue
                    value = disk.getCurrentQueueLength();
                    break;
            }
            total += value;
        }

        // Compute the delta over the summed counter using one shared baseline
        // per operation. JMeter's generate2Samples() computes the display rate
        // client-side from these deltas, same contract as network rx/tx.
        String key = "disk|all|" + operation;
        Long prev = prevDiskCounters.put(key, total);
        if (prev == null) {
            return 0.0; // baseline poll
        }
        return (double) Math.max(0L, total - prev);
    }

    /**
     * Compute the delta between a current counter value and its previous value.
     * Returns 0 on the first call (no baseline yet) and when the counter is
     * unchanged (first poll after a fresh baseline). Returns the difference
     * on subsequent polls, matching the original PerfMon agent behavior where
     * the agent sends delta/rate values rather than absolute counters.
     * JMeter's generate2Samples() then computes the display value from these
     * deltas using the collection interval.
     *
     * @param key     map key identifying the counter (e.g. interface name)
     * @param prevMap map holding previous counter values
     * @param current current counter value
     * @return delta since last poll, or 0 if no baseline yet
     */
    private long computeDelta(Map<String, Long> prevMap, String key, long current) {
        Long prev = prevMap.put(key, current);
        if (prev == null) {
            // First call: store baseline, return 0
            return 0L;
        }
        return Math.max(0L, current - prev);
    }

    public MetricCollector(boolean noExec) {
        this.hardware = systemInfo.getHardware();
        this.operatingSystem = systemInfo.getOperatingSystem();
        this.noExec = noExec;

        // JMX beans
        this.memoryMXBean = ManagementFactory.getMemoryMXBean();
        this.gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
        this.classLoadingBean = ManagementFactory.getClassLoadingMXBean();
        this.compilationBean = ManagementFactory.getCompilationMXBean();
        this.osMXBean = ManagementFactory.getOperatingSystemMXBean();
        this.memoryPoolBeans = ManagementFactory.getMemoryPoolMXBeans();

        log.debug("OSHI MetricCollector initialized (no-exec: {})", noExec);
    }

    /**
     * Execute a single metric specification.
     *
     * @param metricSpec Metric specification string, e.g. "cpu:combined" or "memory:used"
     * @return metric value as a double, or NaN if unavailable
     */
    public double collectMetric(String metricSpec) {
        String[] parts = metricSpec.split(":", 3);
        if (parts.length < 1) {
            log.debug("Empty metric spec: {}", metricSpec);
            return Double.NaN;
        }

        String type = parts[0].trim().toLowerCase().replace(" ", "").replace("/", "");
        String subtype = parts.length > 1 ? parts[1].trim().toLowerCase() : "";
        String selector = parts.length > 2 ? parts[2] : "";

        try {
            switch (type) {
                case "cpu":     return collectCpu(subtype, selector);
                case "memory":  return collectMemory(subtype, selector);
                case "swap":    return collectSwap(subtype, selector);
                case "disks":
                case "disk":
                case "diskio":   return collectDisks(subtype, selector);
                case "network":
                case "networkio": return collectNetwork(subtype, selector);
                case "tcp":     return collectTcp(subtype);
                case "exec":    return collectExec(subtype);
                case "tail":    return collectTail(subtype);
                case "jmx":     return collectJmx(subtype, selector);
                default:
                    // Check for bare network aliases (rx, tx, bytesrecv, etc.) as type
                    if (type.equals("rx") || type.equals("tx") || type.equals("bytesrecv") || type.equals("recvbytes") || type.equals("bytesrx") || type.equals("bytessent") || type.equals("sentbytes") || type.equals("bytestx") || type.equals("rxpackets") || type.equals("packetsrecv") || type.equals("rxpkts") || type.equals("txpackets") || type.equals("packetssent") || type.equals("sentpackets") || type.equals("txpkts") || type.equals("rxerr") || type.equals("errors") || type.equals("rxerrors") || type.equals("errrx") || type.equals("recverrors") || type.equals("rxdrops") || type.equals("drops") || type.equals("rxdropped") || type.equals("dropped") || type.equals("rxdrop")) {
                        return collectNetwork(type, selector);
                    }
                    log.debug("Unknown metric type: {}", type);
                    return Double.NaN;
            }
        } catch (Exception e) {
            log.debug("Error collecting metric [{}:{}] {}", type, subtype, e.getMessage());
            return Double.NaN;
        }
    }

    // ==================== CPU Metrics ====================

    private double collectCpu(String subtype, String selector) {
        CentralProcessor processor = hardware.getProcessor();
        long[] ticks = processor.getSystemCpuLoadTicks();
        if (ticks == null || ticks.length == 0) {
            log.debug("CPU ticks array empty or null");
            return 0.0;
        }
        
        // OSHI getSystemCpuLoadTicks() returns combined CPU ticks (8 values):
        // [user, nice, system, idle, iowait, irq, softirq, steal]
        // These are cumulative across ALL processors, not per-processor.
        long totalTicks = 0;
        long idleTicks = 0;
        for (long tick : ticks) totalTicks += tick;
        if (ticks.length > 3) idleTicks = ticks[3]; // idle is at index 3

        long prevTotal = prevCpuTicks.getOrDefault("total", totalTicks);
        long prevIdle = prevCpuTicks.getOrDefault("idle", idleTicks);
        
        if (prevTotal == totalTicks) {
            // First call: initialize from current ticks, return 0.0 (no baseline yet)
            if (!prevCpuTicks.containsKey("total")) {
                prevCpuTicks.put("total", totalTicks);
                prevCpuTicks.put("idle", idleTicks);
                return 0.0;
            }
            // Ticks unchanged between polls (rare): return cached
            return prevCpuUsage.getOrDefault("total", 0.0);
        }

        double totalDelta = totalTicks - prevTotal;
        double idleDelta = idleTicks - prevIdle;
        double usage = totalDelta > 0 ? (1.0 - (idleDelta / totalDelta)) * 100.0 : 0.0;

        prevCpuTicks.put("total", totalTicks);
        prevCpuTicks.put("idle", idleTicks);
        prevCpuUsage.put("total", usage);

        // Per-component subtypes use the same combined ticks
        long combinedUser = ticks.length > 0 ? ticks[0] : 0;
        long combinedSystem = ticks.length > 2 ? ticks[2] : 0;
        long combinedNice = ticks.length > 1 ? ticks[1] : 0;
        long combinedIowait = ticks.length > 4 ? ticks[4] : 0;
        long combinedIrq = ticks.length > 5 ? ticks[5] : 0;
        long combinedSoftirq = ticks.length > 6 ? ticks[6] : 0;
        long combinedSteal = ticks.length > 7 ? ticks[7] : 0;

        switch (subtype) {
            case "combined":
            case "":
            case "used":
            case "usedperc":
                return usage;
            case "idle":
                return totalDelta > 0 ? (idleDelta / totalDelta) * 100.0 : 0.0;
            case "system": {
                long prevSys = prevCpuTicks.getOrDefault("sys", combinedSystem);
                double sysDelta = combinedSystem - prevSys;
                prevCpuTicks.put("sys", combinedSystem);
                return totalDelta > 0 ? (sysDelta / totalDelta) * 100.0 : 0.0;
            }
            case "user": {
                long prevUser = prevCpuTicks.getOrDefault("usr", combinedUser);
                double userDelta = combinedUser - prevUser;
                prevCpuTicks.put("usr", combinedUser);
                return totalDelta > 0 ? (userDelta / totalDelta) * 100.0 : 0.0;
            }
            case "iowait": {
                long prevIowait = prevCpuTicks.getOrDefault("iow", combinedIowait);
                double iowDelta = combinedIowait - prevIowait;
                prevCpuTicks.put("iow", combinedIowait);
                return totalDelta > 0 ? (iowDelta / totalDelta) * 100.0 : 0.0;
            }
            case "irq": {
                long prevIrq = prevCpuTicks.getOrDefault("irq", combinedIrq);
                double irqDelta = combinedIrq - prevIrq;
                prevCpuTicks.put("irq", combinedIrq);
                return totalDelta > 0 ? (irqDelta / totalDelta) * 100.0 : 0.0;
            }
            case "nice": {
                long prevNice = prevCpuTicks.getOrDefault("nice", combinedNice);
                double niceDelta = combinedNice - prevNice;
                prevCpuTicks.put("nice", combinedNice);
                return totalDelta > 0 ? (niceDelta / totalDelta) * 100.0 : 0.0;
            }
            case "softirq": {
                long prevSi = prevCpuTicks.getOrDefault("si", combinedSoftirq);
                double siDelta = combinedSoftirq - prevSi;
                prevCpuTicks.put("si", combinedSoftirq);
                return totalDelta > 0 ? (siDelta / totalDelta) * 100.0 : 0.0;
            }
            case "stolen": {
                long prevSteal = prevCpuTicks.getOrDefault("steal", combinedSteal);
                double stealDelta = combinedSteal - prevSteal;
                prevCpuTicks.put("steal", combinedSteal);
                return totalDelta > 0 ? (stealDelta / totalDelta) * 100.0 : 0.0;
            }
            default:
                return Double.NaN;
        }
    }

    // ==================== Per-Process CPU ====================

    public double collectPerProcessCpu(String selector, String subtype) {
        int pid = findProcessBySelector(selector);
        if (pid == -1) return Double.NaN;

        OSProcess proc = operatingSystem.getProcess(pid);
        if (proc == null) return Double.NaN;

        String key = "proc:" + pid;
        OSProcess prev = prevProcessSnapshots.get(key);
        prevProcessSnapshots.put(key, proc);

        double cpuUsage = prev != null ? proc.getProcessCpuLoadBetweenTicks(prev) : 0.0;

        switch (subtype.toLowerCase()) {
            case "percent":
            case "total":
                return cpuUsage * 100.0;
            case "user":
                return proc.getUserTime() / 1000.0;
            case "system":
                return proc.getKernelTime() / 1000.0;
            default:
                return cpuUsage * 100.0;
        }
    }

    // ==================== Memory Metrics ====================

    private double collectMemory(String subtype, String selector) {
        GlobalMemory memory = hardware.getMemory();

        switch (subtype.toLowerCase()) {
            case "usedperc":
            case "": {
                long total = memory.getTotal();
                long available = memory.getAvailable();
                return total > 0 ? ((double)(total - available) / total) * 100.0 : 0.0;
            }
            case "freeperc": {
                long total = memory.getTotal();
                long available = memory.getAvailable();
                return total > 0 ? ((double)available / total) * 100.0 : 0.0;
            }
            case "used": {
                long total = memory.getTotal();
                long available = memory.getAvailable();
                return (double)(total - available);
            }
            case "free":
                return (double)memory.getAvailable();
            case "actualfree":
                return (double)memory.getAvailable();
            case "actualused": {
                long total = memory.getTotal();
                long available = memory.getAvailable();
                return (double)(total - available);
            }
            case "ram":
                return (double)memory.getTotal();
            case "total":
                return (double)memory.getTotal();
            case "available":
                return (double)memory.getAvailable();
            default:
                // Per-process memory
                if (selector.contains("name=") || selector.contains("pid=") || selector.contains("ptql=")) {
                    return collectPerProcessMemory(selector, subtype.toLowerCase());
                }
                return Double.NaN;
        }
    }

    private double collectPerProcessMemory(String selector, String subtype) {
        int pid = findProcessBySelector(selector);
        if (pid == -1) return Double.NaN;

        OSProcess proc = operatingSystem.getProcess(pid);
        if (proc == null) return Double.NaN;

        switch (subtype) {
            case "resident":
                return (double)proc.getResidentSetSize();
            case "private":
                return (double)proc.getResidentSetSize();
            case "virtual":
                return (double)proc.getVirtualSize();
            case "shared":
                return 0.0; // Shared memory not available in OSHI 6.6.5 interface
            case "pagefaults":
                return (double)proc.getContextSwitches();
            case "majorfaults":
                return (double)proc.getMajorFaults();
            case "minorfaults":
                return (double)proc.getMinorFaults();
            default:
                return (double)proc.getResidentSetSize();
        }
    }

    // ==================== Swap Metrics ====================

    private double collectSwap(String subtype, String selector) {
        VirtualMemory vmem = hardware.getMemory().getVirtualMemory();

        switch (subtype.toLowerCase()) {
            case "used":
                return (double)vmem.getSwapUsed();
            case "pagein": {
                long pagesIn = vmem.getSwapPagesIn();
                long prev = prevCpuTicks.getOrDefault("swap_in", pagesIn);
                prevCpuTicks.put("swap_in", pagesIn);
                return (double)(pagesIn - prev);
            }
            case "pageout": {
                long pagesOut = vmem.getSwapPagesOut();
                long prev = prevCpuTicks.getOrDefault("swap_out", pagesOut);
                prevCpuTicks.put("swap_out", pagesOut);
                return (double)(pagesOut - prev);
            }
            case "free":
                return (double)(vmem.getSwapTotal() - vmem.getSwapUsed());
            case "total":
                return (double)vmem.getSwapTotal();
            case "usedperc":
                {
                    long total = vmem.getSwapTotal();
                    if (total <= 0) return 0.0;
                    return ((double)vmem.getSwapUsed() / (double)total) * 100.0;
                }
            default:
                return (double)vmem.getSwapUsed();
        }
    }

    // ==================== Disk I/O Metrics ====================

    private double collectDisks(String subtype, String selector) {
        // Re-assemble the original spec so the single collector handles both
        // selector positions (subtype=fs=/path or selector=fs=/path).
        String spec = "disks";
        if (subtype != null && !subtype.isEmpty()) spec += ":" + subtype;
        if (selector != null && !selector.isEmpty()) spec += ":" + selector;
        return collectDiskIo(spec);
    }

    private double collectFilesystem(String subtype, String selector) {
        FileSystem fileSystem = operatingSystem.getFileSystem();
        List<OSFileStore> stores = fileSystem.getFileStores();

        // selector may be null (no fs= mount given) or empty — treat both as "all stores".
        String sel = (selector != null && !selector.isEmpty())
            ? (selector.startsWith("fs=") ? selector.substring(3) : selector)
            : null;

        for (OSFileStore store : stores) {
            if (sel != null) {
                boolean match = (store.getName() != null && store.getName().contains(sel))
                    || (store.getMount() != null && store.getMount().contains(sel))
                    || sel.equals("/");
                if (!match) continue;
            }

            switch (subtype.toLowerCase()) {
                case "total":
                    return (double)store.getTotalSpace();
                case "used":
                    return (double)(store.getTotalSpace() - store.getUsableSpace());
                case "free":
                case "available":
                    return (double)store.getUsableSpace();
                case "useperc": {
                    long total = store.getTotalSpace();
                    if (total == 0) return 0.0;
                    return ((double)(total - store.getUsableSpace()) / total) * 100.0;
                }
                case "files":
                    return (double)store.getTotalInodes();
                case "freefiles":
                    return (double)store.getFreeInodes();
                default:
                    return 0.0;
            }
        }
        return 0.0;
    }

    // ==================== Network I/O Metrics ====================

    private double collectNetwork(String subtype, String selector) {
        List<NetworkIF> networkIfs = hardware.getNetworkIFs();

        double totalRx = 0, totalTx = 0, totalRxPkt = 0, totalTxPkt = 0;
        double totalErr = 0, totalQueue = 0;
        double maxSpeed = 0;
        boolean anyReal = false;

        for (NetworkIF iface : networkIfs) {
            String name = iface.getName();
            // Skip loopback and docker/virtual interfaces
            if (name.equals("lo") || name.startsWith("docker") || name.startsWith("veth") ||
                name.startsWith("br-") || name.startsWith("virbr") || name.startsWith("vnet") ||
                name.startsWith("tun") || name.startsWith("tap")) {
                continue;
            }
            if (!selector.isEmpty() && !name.contains(selector)) continue;

            anyReal = true;
            long recv = iface.getBytesRecv();
            long sent = iface.getBytesSent();
            long rxPkts = iface.getPacketsRecv();
            long txPkts = iface.getPacketsSent();
            long err = iface.getInErrors();
            long queue = 0; // OSHI 6.x NetworkIF doesn't expose queue length
            long speed = iface.getSpeed();

            switch (subtype.toLowerCase()) {
                case "bytesrecv":
                case "rx":
                case "rxbytes":
                case "recvbytes":
                case "bytesrx":
                    {
                    prevNetRx.put(name, recv);
                    return (double)recv; // Always absolute value
                }
                case "bytessent":
                case "tx":
                case "txbytes":
                case "sentbytes":
                case "bytestx":
                    {
                    prevNetTx.put(name, sent);
                    return (double)sent; // Always absolute value
                }
                case "packetsrecv":
                case "rxpackets":
                case "rxpkts":
                case "recvpackets":
                case "pktsrecv":
                    {
                    prevNetPktRx.put(name, rxPkts);
                    return (double)rxPkts; // Always absolute value
                }
                case "packetssent":
                case "txpackets":
                case "txpkts":
                case "sentpackets":
                case "pktsent":
                    {
                    prevNetPktTx.put(name, txPkts);
                    return (double)txPkts; // Always absolute value
                }
                case "rxerr":
                case "errors":
                case "rxerrors":
                case "errrx":
                case "recverrors":
                    {
                    prevNetErr.put(name, err);
                    return (double)err; // Always absolute value
                }
                case "rxdrops":
                case "drops":
                case "rxdropped":
                case "dropped":
                case "rxdrop":
                    {
                    long drop = 0;
                    prevNetDrop.put(name, drop);
                    return 0.0;
                }
                case "speed":
                    return (double)speed;
                case "queue":
                    totalQueue += queue;
                    break;
                case "used":
                case "":
                    return (double)(recv + sent);
                default:
                    return 0.0;
            }
        }
        return 0.0;
    }

    /**
     * Collect network metrics and return rx|tx|rxpkts|txpkts as pipe-separated
     * delta values. Each value is the change since the last poll (0 on first
     * poll). JMeter's generate2Samples() uses these deltas to compute display
     * values via the collection interval.
     *
     * @param metric The full metric spec (e.g. "network:rx" or "network:rxbytes")
     * @return Pipe-separated "rx_delta|tx_delta|rxpkts_delta|txpkts_delta" string
     */
    public String collectNetworkPair(String metric) {
        String[] parts = metric.split(":", 3);
        String selector = parts.length >= 3 ? parts[2] : "";

        long totalRx = 0, totalTx = 0, totalRxPkt = 0, totalTxPkt = 0;
        boolean anyReal = false;

        List<NetworkIF> networkIfs = hardware.getNetworkIFs();
        for (NetworkIF iface : networkIfs) {
            String name = iface.getName();
            if (name.equals("lo") || name.startsWith("docker") || name.startsWith("veth") ||
                name.startsWith("br-") || name.startsWith("virbr") || name.startsWith("vnet") ||
                name.startsWith("tun") || name.startsWith("tap")) {
                continue;
            }
            if (!selector.isEmpty() && !name.contains(selector)) continue;

            anyReal = true;
            totalRx += computeDelta(prevNetRx, name, iface.getBytesRecv());
            totalTx += computeDelta(prevNetTx, name, iface.getBytesSent());
            totalRxPkt += computeDelta(prevNetPktRx, name, iface.getPacketsRecv());
            totalTxPkt += computeDelta(prevNetPktTx, name, iface.getPacketsSent());
        }

        if (!anyReal) {
            return "0.000000|0.000000|0.000000|0.000000";
        }

        return String.format("%.6f|%.6f|%.6f|%.6f", (double) totalRx, (double) totalTx,
                             (double) totalRxPkt, (double) totalTxPkt);
    }

    /**
     * Collect RX bytes as delta (change since last poll) for all non-virtual
     * interfaces matching the selector. Returns the total delta across all
     * matching interfaces. The first poll returns 0 (baseline establishment).
     * JMeter's generate2Samples() uses these deltas to compute display values.
     */
    public double collectNetworkRx(String selector) {
        long total = 0;
        List<NetworkIF> networkIfs = hardware.getNetworkIFs();

        for (NetworkIF iface : networkIfs) {
            String name = iface.getName();
            if (name.equals("lo") || name.startsWith("docker") || name.startsWith("veth") ||
                name.startsWith("br-") || name.startsWith("virbr") || name.startsWith("vnet") ||
                name.startsWith("tun") || name.startsWith("tap")) {
                continue;
            }
            if (!selector.isEmpty() && !name.contains(selector)) continue;
            total += computeDelta(prevNetRx, name, iface.getBytesRecv());
        }
        return (double) total;
    }

    /**
     * Collect TX bytes as delta (change since last poll) for all non-virtual
     * interfaces matching the selector. Returns the total delta across all
     * matching interfaces. The first poll returns 0 (baseline establishment).
     */
    public double collectNetworkTx(String selector) {
        long total = 0;
        List<NetworkIF> networkIfs = hardware.getNetworkIFs();

        for (NetworkIF iface : networkIfs) {
            String name = iface.getName();
            if (name.equals("lo") || name.startsWith("docker") || name.startsWith("veth") ||
                name.startsWith("br-") || name.startsWith("virbr") || name.startsWith("vnet") ||
                name.startsWith("tun") || name.startsWith("tap")) {
                continue;
            }
            if (!selector.isEmpty() && !name.contains(selector)) continue;
            total += computeDelta(prevNetTx, name, iface.getBytesSent());
        }
        return (double) total;
    }

    /**
     * Collect combined network usage (rx+tx delta) for 'used' subtype.
     * Returns total delta of rx and tx bytes since last poll.
     */
    public double collectNetworkUsed(String selector) {
        long totalRx = 0, totalTx = 0;
        List<NetworkIF> networkIfs = hardware.getNetworkIFs();

        for (NetworkIF iface : networkIfs) {
            String name = iface.getName();
            if (name.equals("lo") || name.startsWith("docker") || name.startsWith("veth") ||
                name.startsWith("br-") || name.startsWith("virbr") || name.startsWith("vnet") ||
                name.startsWith("tun") || name.startsWith("tap")) {
                continue;
            }
            if (!selector.isEmpty() && !name.contains(selector)) continue;
            totalRx += computeDelta(prevNetRx, name, iface.getBytesRecv());
            totalTx += computeDelta(prevNetTx, name, iface.getBytesSent());
        }
        return (double) (totalRx + totalTx);
    }

    /**
     * Collect RX packets as delta (change since last poll).
     * Returns total delta of received packets since last poll.
     */
    String collectNetworkPacketsRx(String selector) {
        long total = 0;
        List<NetworkIF> networkIfs = hardware.getNetworkIFs();
        for (NetworkIF iface : networkIfs) {
            String name = iface.getName();
            if (name.equals("lo") || name.startsWith("docker") || name.startsWith("veth") ||
                name.startsWith("br-") || name.startsWith("virbr") || name.startsWith("vnet") ||
                name.startsWith("tun") || name.startsWith("tap")) continue;
            if (!selector.isEmpty() && !name.contains(selector)) continue;
            total += computeDelta(prevNetPktRx, name, iface.getPacketsRecv());
        }
        return String.format("%.6f", (double) total);
    }

    /**
     * Collect TX packets as delta (change since last poll).
     * Returns total delta of sent packets since last poll.
     */
    String collectNetworkPacketsTx(String selector) {
        long total = 0;
        List<NetworkIF> networkIfs = hardware.getNetworkIFs();
        for (NetworkIF iface : networkIfs) {
            String name = iface.getName();
            if (name.equals("lo") || name.startsWith("docker") || name.startsWith("veth") ||
                name.startsWith("br-") || name.startsWith("virbr") || name.startsWith("vnet") ||
                name.startsWith("tun") || name.startsWith("tap")) continue;
            if (!selector.isEmpty() && !name.contains(selector)) continue;
            total += computeDelta(prevNetPktTx, name, iface.getPacketsSent());
        }
        return String.format("%.6f", (double) total);
    }

    /**
     * Collect RX errors as delta (change since last poll).
     * Returns total delta of receive errors since last poll.
     */
    String collectNetworkErrors(String selector) {
        long total = 0;
        List<NetworkIF> networkIfs = hardware.getNetworkIFs();
        for (NetworkIF iface : networkIfs) {
            String name = iface.getName();
            if (name.equals("lo") || name.startsWith("docker") || name.startsWith("veth") ||
                name.startsWith("br-") || name.startsWith("virbr") || name.startsWith("vnet") ||
                name.startsWith("tun") || name.startsWith("tap")) continue;
            if (!selector.isEmpty() && !name.contains(selector)) continue;
            total += computeDelta(prevNetErr, name, iface.getInErrors());
        }
        return String.format("%.6f", (double) total);
    }

    /**
     * Collect RX drops as delta (change since last poll).
     * Returns total delta of receive drops since last poll.
     */
    String collectNetworkDrops(String selector) {
        long total = 0;
        List<NetworkIF> networkIfs = hardware.getNetworkIFs();
        for (NetworkIF iface : networkIfs) {
            String name = iface.getName();
            if (name.equals("lo") || name.startsWith("docker") || name.startsWith("veth") ||
                name.startsWith("br-") || name.startsWith("virbr") || name.startsWith("vnet") ||
                name.startsWith("tun") || name.startsWith("tap")) continue;
            if (!selector.isEmpty() && !name.contains(selector)) continue;
            total += computeDelta(prevNetDrop, name, iface.getInDrops());
        }
        return String.format("%.6f", (double) total);
    }

    /**
     * Collect max network speed (single value).
     */
    String collectNetworkSpeed(String selector) {
        double maxSpeed = 0;
        List<NetworkIF> networkIfs = hardware.getNetworkIFs();
        for (NetworkIF iface : networkIfs) {
            String name = iface.getName();
            if (name.equals("lo") || name.startsWith("docker") || name.startsWith("veth") ||
                name.startsWith("br-") || name.startsWith("virbr") || name.startsWith("vnet") ||
                name.startsWith("tun") || name.startsWith("tap")) continue;
            if (!selector.isEmpty() && !name.contains(selector)) continue;
            long speed = iface.getSpeed();
            if (speed > 0 && speed > maxSpeed) maxSpeed = speed;
        }
        return String.format("%.6f", maxSpeed);
    }

    /**
     * Collect a metric value(s) and return as string.
     * For network rx/tx/bytes packets/errors, returns single value (original PerfMon compatibility).
     * For network:used, returns combined rx+tx.
     */
    public String collectMetricAsString(String metric) {
        // Parse once; disk I/O (JMeter sends "disks i/o") and network I/O
        // (JMeter sends "network i/o") need the raw spec for their collectors.
        String[] parts = metric.split(":", 3);
        String typeNorm = parts[0].trim().toLowerCase().replace(" ", "").replace("/", "");
        if (typeNorm.equals("disks") || typeNorm.equals("disk") || typeNorm.equals("diskio")
            || typeNorm.equals("disksio")) {
            double value = collectDiskIo(metric);
            return String.format("%.6f", value);
        }
        // Check if network metric (strip spaces and slashes: JMeter sends "network i/o")
        if (parts.length >= 2 && typeNorm.equals("network")) {
            String subtype = parts[1].toLowerCase();
            String selector = parts.length >= 3 ? parts[2] : "";
            
            // Single-value network metrics (original PerfMon compatibility)
            if (subtype.equals("rx") || subtype.equals("bytesrecv") || subtype.equals("recvbytes") || subtype.equals("bytesrx") || subtype.equals("rxbytes")) {
                return String.format("%.6f", collectNetworkRx(selector));
            }
            if (subtype.equals("tx") || subtype.equals("bytessent") || subtype.equals("sentbytes") || subtype.equals("bytestx") || subtype.equals("txbytes")) {
                return String.format("%.6f", collectNetworkTx(selector));
            }
            if (subtype.equals("rxpackets") || subtype.equals("packetsrecv") || subtype.equals("rxpkts") || subtype.equals("recvpackets")) {
                return collectNetworkPacketsRx(selector);
            }
            if (subtype.equals("txpackets") || subtype.equals("packetssent") || subtype.equals("txpkts") || subtype.equals("sentpackets")) {
                return collectNetworkPacketsTx(selector);
            }
            if (subtype.equals("errors") || subtype.equals("rxerr") || subtype.equals("rxerrors") || subtype.equals("errrx") || subtype.equals("recverrors")) {
                return collectNetworkErrors(selector);
            }
            if (subtype.equals("rxdrops") || subtype.equals("drops") || subtype.equals("rxdropped") || subtype.equals("dropped") || subtype.equals("rxdrop")) {
                return collectNetworkDrops(selector);
            }
            if (subtype.equals("used")) {
                return String.format("%.6f", collectNetworkUsed(selector));
            }
            if (subtype.isEmpty()) {
                // Bare "network" with no subtype: return combined rx+tx (same as "used")
                return String.format("%.6f", collectNetworkUsed(selector));
            }
            if (subtype.equals("speed")) {
                return collectNetworkSpeed(selector);
            }
            if (subtype.equals("queue")) {
                return "0.000000";
            }
        }
        // Non-network metric: return single value
        double value = collectMetric(metric);
        return String.format("%.6f", value);
    }

    // ==================== TCP Metrics ====================

    private double collectTcp(String subtype) {
        try {
            TcpStats tcpStats = operatingSystem.getInternetProtocolStats().getTCPv4Stats();
            switch (subtype.toLowerCase()) {
                case "estab":
                case "":
                    return (double)tcpStats.getConnectionsEstablished();
                case "time_wait":
                    return countConnectionsByState(TcpState.TIME_WAIT);
                case "close_wait":
                    return countConnectionsByState(TcpState.CLOSE_WAIT);
                case "listen":
                    return countConnectionsByState(TcpState.LISTEN);
                case "syn_recv":
                    return countConnectionsByState(TcpState.SYN_RECV);
                case "close":
                    return countConnectionsByState(TcpState.CLOSED);
                case "fin_wait1":
                    return countConnectionsByState(TcpState.FIN_WAIT_1);
                case "fin_wait2":
                    return countConnectionsByState(TcpState.FIN_WAIT_2);
                case "last_ack":
                    return countConnectionsByState(TcpState.LAST_ACK);
                case "bound":
                    return countConnectionsByState(TcpState.SYN_SENT);
                case "idle":
                    return countConnectionsByState(TcpState.CLOSED);
                case "closing":
                    return countConnectionsByState(TcpState.CLOSING);
                case "outbound":
                    return (double)tcpStats.getConnectionsActive();
                case "inbound":
                    return (double)tcpStats.getConnectionsPassive();
                default:
                    return 0.0;
            }
        } catch (Exception e) {
            return 0.0;
        }
    }

    private double countConnectionsByState(TcpState state) {
        long count = 0;
        for (IPConnection conn : operatingSystem.getInternetProtocolStats().getConnections()) {
            if (conn.getType().startsWith("tcp") && conn.getState() == state) {
                count++;
            }
        }
        return (double)count;
    }

    // ==================== Custom: EXEC ====================

    private double collectExec(String command) {
        if (noExec) return Double.NaN;

        try {
            ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", command);
            pb.redirectErrorStream(true);
            Process process = pb.start();

            java.io.BufferedReader reader = new java.io.BufferedReader(
                new java.io.InputStreamReader(process.getInputStream()));
            String line = reader.readLine();
            process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS);

            if (line != null) {
                return Double.parseDouble(line.trim());
            }
        } catch (NumberFormatException e) {
            log.debug("Exec returned non-numeric: {}", e.getMessage());
        } catch (Exception e) {
            log.debug("Exec failed [{}]: {}", command, e.getMessage());
        }
        return Double.NaN;
    }

    // ==================== Custom: TAIL ====================

    private double collectTail(String filePath) {
        try {
            java.nio.file.Path path = java.nio.file.Paths.get(filePath);
            if (!java.nio.file.Files.exists(path)) return Double.NaN;

            List<String> lines = java.nio.file.Files.readAllLines(path);
            if (lines.isEmpty()) return Double.NaN;

            String lastLine = lines.get(lines.size() - 1).trim();
            return Double.parseDouble(lastLine);
        } catch (NumberFormatException e) {
            return Double.NaN;
        } catch (Exception e) {
            log.debug("Tail failed [{}]: {}", filePath, e.getMessage());
            return Double.NaN;
        }
    }

    // ==================== JMX Metrics ====================

    private double collectJmx(String type, String selector) {
        switch (type.toLowerCase()) {
            case "gc-time": {
                long totalGcTime = 0;
                for (GarbageCollectorMXBean bean : gcBeans) {
                    totalGcTime += bean.getCollectionTime();
                }
                return (double)totalGcTime;
            }
            case "memory-usage": {
                MemoryUsage usage = memoryMXBean.getHeapMemoryUsage();
                return usage != null ? (double)usage.getUsed() : 0.0;
            }
            case "memory-committed": {
                MemoryUsage usage = memoryMXBean.getHeapMemoryUsage();
                return usage != null ? (double)usage.getCommitted() : 0.0;
            }
            case "memorypool-usage": {
                double total = 0;
                for (MemoryPoolMXBean pool : memoryPoolBeans) {
                    MemoryUsage usage = pool.getUsage();
                    if (usage != null) total += usage.getUsed();
                }
                return total;
            }
            case "memorypool-committed": {
                double total = 0;
                for (MemoryPoolMXBean pool : memoryPoolBeans) {
                    MemoryUsage usage = pool.getUsage();
                    if (usage != null) total += usage.getCommitted();
                }
                return total;
            }
            case "class-count":
                return classLoadingBean != null ? (double)classLoadingBean.getLoadedClassCount() : 0.0;
            case "compile-time":
                return compilationBean != null
                    ? compilationBean.getTotalCompilationTime() : 0.0;
            default:
                return Double.NaN;
        }
    }

    // ==================== Process Selection ====================

    private int findProcessBySelector(String selector) {
        List<OSProcess> procs = operatingSystem.getProcesses();

        if (selector.contains("pid=")) {
            String pidStr = selector.substring(selector.indexOf("pid=") + 4);
            try {
                return Integer.parseInt(pidStr);
            } catch (NumberFormatException e) {
                return -1;
            }
        }

        if (selector.contains("name=")) {
            String name = selector.substring(selector.indexOf("name=") + 5);
            // Handle "#N" suffix for nth match
            int nthMatch = 1;
            if (name.contains("#")) {
                String[] parts = name.split("#");
                name = parts[0];
                nthMatch = Integer.parseInt(parts[1]);
            }

            int found = 0;
            for (OSProcess proc : procs) {
                if (proc.getName().toLowerCase().contains(name.toLowerCase())) {
                    found++;
                    if (found == nthMatch) return proc.getProcessID();
                }
            }
        }

        if (selector.contains("ptql=")) {
            String query = selector.substring(selector.indexOf("ptql=") + 5);
            // Simple PTQL: parent=PARENT_PID
            if (query.startsWith("parent=")) {
                int parentPid = Integer.parseInt(query.substring(7));
                for (OSProcess proc : procs) {
                    if (proc.getParentProcessID() == parentPid) {
                        return proc.getProcessID();
                    }
                }
            }
        }

        return -1;
    }

    // ==================== System Info Listing ====================

    public List<String> listProcesses() {
        List<OSProcess> procs = operatingSystem.getProcesses();
        List<String> result = new ArrayList<>();
        for (OSProcess proc : procs) {
            if (proc != null) {
                result.add("Process: pid=" + proc.getProcessID() + " name=" + proc.getName()
                    + (proc.getCommandLine() != null ? " args=" + proc.getCommandLine() : ""));
            }
        }
        return result;
    }

    public List<String> listFilesystems() {
        FileSystem fileSystem = operatingSystem.getFileSystem();
        List<String> result = new ArrayList<>();
        for (OSFileStore store : fileSystem.getFileStores()) {
            result.add("Filesystem: fs=" + store.getName() + " type=" + store.getType()
                + " mount=" + store.getMount());
        }
        return result;
    }

    public List<String> listNetworkInterfaces() {
        List<NetworkIF> ifs = hardware.getNetworkIFs();
        List<String> result = new ArrayList<>();
        for (NetworkIF iface : ifs) {
            String addr = "";
            try {
                String[] addrs = iface.getIPv4addr();
                if (addrs.length > 0) addr = " addr=" + addrs[0];
            } catch (Exception ignored) {}
            result.add("Network interface: iface=" + iface.getName() + addr
                + " type=" + (safeIsLoopback(iface) ? "Local Loopback" : "Ethernet"));
        }
        return result;
    }

    private boolean safeIsLoopback(NetworkIF iface) {
        try {
            return iface.queryNetworkInterface().isLoopback();
        } catch (Exception e) {
            return false;
        }
    }
}
