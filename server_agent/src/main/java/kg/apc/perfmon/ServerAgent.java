package kg.apc.perfmon;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * PerfMon Server Agent — OSHI-based replacement for Sigar.
 * Compatible with original PerfMon listener protocol.
 *
 * Protocol:
 *   TCP/UDP plain-text, colon-separated parameters, tab-separated values in responses
 *   Commands: test, exit, shutdown, interval:N, metrics:N, metrics-single:N
 *
 * Usage:
 *   java -jar ServerAgent.jar [--tcp-port PORT] [--udp-port PORT] [--interval SECONDS]
 *                              [--no-exec] [--auto-shutdown] [--sysinfo] [--loglevel LEVEL]
 */
public class ServerAgent {

    private static final Logger log = LogManager.getLogger(ServerAgent.class);

    private static final String VERSION = "3.0.0";

    public static void main(String[] args) {
        ArgsParser parser = new ArgsParser(args);

        log.info("PerfMon Server Agent v{} starting", VERSION);
        log.info("OSHI-based agent (replaces Sigar)");

        int tcpPort = parser.getTcpPort();
        int udpPort = parser.getUdpPort();
        int interval = parser.getInterval();
        boolean noExec = parser.isNoExec();
        boolean autoShutdown = parser.isAutoShutdown();
        boolean sysinfo = parser.isSysinfo();

        // Print system info and exit if requested
        if (sysinfo) {
            printSysinfo();
            return;
        }

        if (tcpPort == 0 && udpPort == 0) {
            System.err.println("Error: Both TCP and UDP ports are set to 0. At least one must be non-zero.");
            System.err.println("Usage: java -jar ServerAgent.jar [--tcp-port PORT] [--udp-port PORT]");
            System.err.println("Use 0 to disable a protocol (default 4444 for both).");
            System.exit(1);
        }

        MetricCollector collector = new MetricCollector(noExec);
        MetricsStore metricsStore = new MetricsStore(interval);

        AtomicBoolean running = new AtomicBoolean(true);

        // Create TcpServer first (needed for ProtocolHandler reference)
        TcpServer tcpServer = null;
        if (tcpPort > 0) {
            tcpServer = new TcpServer(tcpPort, null, metricsStore, autoShutdown, running);
            tcpServer.start();
            log.info("Binding TCP to {}", tcpPort);
        }

        ProtocolHandler handler = new ProtocolHandler(collector, metricsStore, tcpServer);

        // Wire handler into TcpServer after creation
        if (tcpServer != null) {
            tcpServer.setHandler(handler);
        }

        // UDP server
        if (udpPort > 0) {
            UdpServer udpServer = new UdpServer(udpPort, handler);
            udpServer.start();
            log.info("Binding UDP to {}", udpPort);
        }

        log.info("Agent ready. Press Ctrl+C to stop.");
        log.info("Auto-shutdown: {}", autoShutdown);

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down...");
            running.set(false);
            metricsStore.shutdown();
        }));

        // Block main thread
        while (running.get()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private static void printSysinfo() {
        MetricCollector collector = new MetricCollector(false);

        System.out.println("=== Available Processes ===");
        collector.listProcesses().forEach(p ->
            System.out.println("Process: " + p)
        );

        System.out.println("\n=== Available Filesystems ===");
        collector.listFilesystems().forEach(f ->
            System.out.println("Filesystem: " + f)
        );

        System.out.println("\n=== Available Network Interfaces ===");
        collector.listNetworkInterfaces().forEach(n ->
            System.out.println("Network interface: " + n)
        );

        System.out.println("\n=== Done logging sysinfo ===");
    }
}
