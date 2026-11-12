package kg.apc.perfmon;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handles the PerfMon protocol commands.
 * 
 * Protocol:
 *   Plain text, one command per line
 *   Commands: test, exit, shutdown, interval:N, metrics:N, metrics-single:N
 *   Metric format: type:subtype:selector
 *   Multiple metrics separated by TAB
 */
public class ProtocolHandler {

    private static final Logger log = LogManager.getLogger(ProtocolHandler.class);

    private final MetricCollector collector;
    private final MetricsStore metricsStore;
    TcpServer tcpServer;
    private final ConcurrentHashMap<String, ClientMetrics> clients = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LinkedBlockingQueue<String>> clientQueues = new ConcurrentHashMap<>();
    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

    // Counter for client IDs
    private int clientCounter = 0;

    public ProtocolHandler(MetricCollector collector, MetricsStore metricsStore, TcpServer tcpServer) {
        this.collector = collector;
        this.metricsStore = metricsStore;
        this.tcpServer = tcpServer;
    }

    public TcpServer getTcpServer() { return tcpServer; }

    /**
     * Register a client queue from TcpServer.
     */
    public void registerClientQueue(String clientId, LinkedBlockingQueue<String> queue) {
        clientQueues.put(clientId, queue);
    }

    /**
     * Process a command received from a client.
     * 
     * @param clientIdentifier Client ID (for session tracking)
     * @param commandRaw Raw command string
     * @return Response string, or null if client should disconnect
     */
    public String processCommand(String clientIdentifier, String commandRaw) {
        String command = commandRaw.trim();
        if (command.isEmpty()) return "Ok\n";

        log.debug("Received command: {} (raw len={})", command, commandRaw.length());

        // Use untrimmed commandRaw for metrics param to preserve trailing tabs (JMeter sends metrics:rx\t)
        // Use trimmed command for comparison (exit, test, shutdown, interval)
        String trimmedCommand = commandRaw.trim();

        if (trimmedCommand.equals("exit")) {
            return handleExit(clientIdentifier);
        }

        if (trimmedCommand.equals("test")) {
            return "Yep\n";
        }

        if (trimmedCommand.equals("shutdown")) {
            shutdownRequested.set(true);
            // Stop all metrics sessions
            for (ClientMetrics cm : clients.values()) {
                metricsStore.stopMetrics(cm);
            }
            clients.clear();
            clientQueues.clear();
            return "Ok\n";
        }

        if (trimmedCommand.startsWith("interval:")) {
            String intervalStr = trimmedCommand.substring("interval:".length()).trim();
            try {
                int interval = Integer.parseInt(intervalStr);
                if (interval < 1) interval = 1;
                metricsStore.setInterval(interval);
                return ""; // No response — JMeter reads the next line (metrics values)
            } catch (NumberFormatException e) {
                return "Error: invalid interval '" + intervalStr + "'\n";
            }
        }

        if (commandRaw.startsWith("metrics:")) {
            // Use untrimmed commandRaw to preserve trailing tabs (JMeter sends metrics:rx\t)
            String metricsParam = commandRaw.substring("metrics:".length());
            return handleMetrics(clientIdentifier, metricsParam);
        }

        if (command.startsWith("metrics-single:")) {
            return handleMetricsSingle(command.substring("metrics-single:".length()));
        }

        return "Error: unknown command '" + command + "'\n";
    }

    private String handleExit(String clientId) {
        // Don't remove client from map — handleMetrics needs to find it to stop scheduler
        // ClientMetrics cm = clients.remove(clientId);
        ClientMetrics cm = clients.get(clientId);
        if (cm != null) {
            // Don't stop metrics — the client sends exit but keeps reading values
            // from the socket. The metrics polling continues until the socket closes.
            log.info("Client {} sent exit (metrics keep running)", clientId);
        }
        // Don't remove queue — new metrics sessions reuse it
        // clientQueues.remove(clientId);
        return null; // Signal disconnect (stop reading commands)
    }

    private String handleMetrics(String clientId, String metricsParam) {
        List<String> metricList = parseMetricsList(metricsParam);
        log.info("handleMetrics: raw param='{}', parsed metrics={}", metricsParam, metricList);
        
        // Check if already running and stop it
        ClientMetrics existing = clients.get(clientId);
        if (existing != null && existing.getRunning().get()) {
            // Stop previous session's scheduler
            metricsStore.stopMetrics(existing);
            clients.remove(clientId);
        }

        // Get the shared queue from TcpServer's ClientThread
        LinkedBlockingQueue<String> queue = clientQueues.get(clientId);
        if (queue == null) {
            log.warn("No queue registered for client {}", clientId);
            return "Error: no connection\n";
        }

        ClientMetrics cm = new ClientMetrics(clientId, collector, metricList, queue);
        clients.put(clientId, cm);
        cm.getRunning().set(true);

        // Tell TcpServer's client thread about this metrics session
        tcpServer.setActiveMetrics(clientId, cm);

        // Start continuous polling for subsequent updates
        // Warm-up will queue the first result (replaces initial result)
        metricsStore.startMetrics(cm);

        log.info("Client {} started metrics: {}", clientId, metricList);
        return "";
    }

    private String handleMetricsSingle(String metricsParam) {
        List<String> metricList = parseMetricsList(metricsParam);

        StringBuilder result = new StringBuilder();
        for (int i = 0; i < metricList.size(); i++) {
            String metric = metricList.get(i);
            if (isDiskMetric(metric)) {
                if (result.length() > 0) result.append("\t");
                result.append(String.format("%.6f", collector.collectDiskIo(metric)));
            } else if (isNetworkMetric(metric)) {
                String netValue = collectNetworkSingleValue(metric);
                if (result.length() > 0) result.append("\t");
                result.append(netValue);
            } else {
                double value = collector.collectMetric(metric);
                if (result.length() > 0) result.append("\t");
                result.append(String.format("%.6f", value));
            }
        }

        return result.toString() + "\n";
    }

    /**
     * Collect a single network metric value for standalone queries.
     * Returns single value for rx/tx, pair for used/speed.
     */
    private String collectNetworkSingleValue(String metric) {
        String[] parts = metric.split(":", 3);
        String subtype = parts.length >= 2 ? parts[1].toLowerCase() : "";
        String selector = parts.length >= 3 ? parts[2] : "";
        
        // Single-value network metrics
        if (subtype.equals("rx") || subtype.equals("bytesrecv") || subtype.equals("recvbytes") || subtype.equals("bytesrx") || subtype.equals("rxbytes")) {
            return String.format("%.6f", collector.collectNetworkRx(selector));
        }
        if (subtype.equals("tx") || subtype.equals("bytessent") || subtype.equals("sentbytes") || subtype.equals("bytestx") || subtype.equals("txbytes")) {
            return String.format("%.6f", collector.collectNetworkTx(selector));
        }
        if (subtype.equals("rxpackets") || subtype.equals("packetsrecv") || subtype.equals("rxpkts") || subtype.equals("recvpackets")) {
            return collector.collectNetworkPacketsRx(selector);
        }
        if (subtype.equals("txpackets") || subtype.equals("packetssent") || subtype.equals("txpkts") || subtype.equals("sentpackets")) {
            return collector.collectNetworkPacketsTx(selector);
        }
        if (subtype.equals("errors") || subtype.equals("rxerr") || subtype.equals("rxerrors") || subtype.equals("errrx") || subtype.equals("recverrors")) {
            return collector.collectNetworkErrors(selector);
        }
        if (subtype.equals("rxdrops") || subtype.equals("drops") || subtype.equals("rxdropped") || subtype.equals("dropped") || subtype.equals("rxdrop")) {
            return collector.collectNetworkDrops(selector);
        }
        if (subtype.equals("used") || subtype.isEmpty()) {
            return String.format("%.6f", collector.collectNetworkUsed(selector));
        }
        if (subtype.equals("speed")) {
            return collector.collectNetworkSpeed(selector);
        }
        if (subtype.equals("queue")) {
            return "0.000000";
        }
        // Default: return pair for unknown network metrics
        return collector.collectNetworkPair(metric);
    }

    /**
     * Check if a metric spec is a disk I/O metric. JMeter sends "disks i/o"
     * (with space and slash), so the type must be normalized before matching.
     */
    private boolean isDiskMetric(String metric) {
        String[] parts = metric.toLowerCase().split(":");
        if (parts.length < 2) return false;
        String typeNorm = parts[0].trim().replace(" ", "").replace("/", "");
        return typeNorm.equals("disks") || typeNorm.equals("disk") || typeNorm.equals("diskio")
            || typeNorm.equals("disksio");
    }

    /**
     * Check if a metric spec is a network rx/tx metric.
     * Network rx/tx metrics always return both rx and tx values.
     */
    private boolean isNetworkMetric(String metric) {
        String[] parts = metric.toLowerCase().split(":");
        if (parts.length < 1) return false;
        // Strip spaces and slashes: JMeter sends "network i/o"
        String typeNorm = parts[0].trim().replace(" ", "").replace("/", "");
        if (!typeNorm.equals("network")) return false;
        if (parts.length < 2) return false;
        String subtype = parts[1];
        // rx/tx/bytesrecv/recvbytes/bytesrx/rxbytes/txbytes/bytessent/sentbytes/bytestx
        // rxpackets/txpackets/packetsrecv/packetssent
        // rxerr/errors/rxerrors/errrx/recverrors
        // rxdrops/drops/rxdropped/dropped/rxdrop
        return subtype.equals("rx") || subtype.equals("tx")
            || subtype.equals("bytesrecv") || subtype.equals("recvbytes") || subtype.equals("bytesrx")
            || subtype.equals("rxbytes") || subtype.equals("txbytes")
            || subtype.equals("bytessent") || subtype.equals("sentbytes") || subtype.equals("bytestx")
            || subtype.equals("rxpackets") || subtype.equals("packetsrecv") || subtype.equals("rxpkts")
            || subtype.equals("txpackets") || subtype.equals("packetssent") || subtype.equals("sentpackets") || subtype.equals("txpkts")
            || subtype.equals("rxerr") || subtype.equals("errors") || subtype.equals("rxerrors")
            || subtype.equals("errrx") || subtype.equals("recverrors")
            || subtype.equals("rxdrops") || subtype.equals("drops") || subtype.equals("rxdropped")
            || subtype.equals("dropped") || subtype.equals("rxdrop");
    }

    /**
     * Parse the metrics parameter string into individual metric specs.
     * Format: metric1<TAB>metric2<TAB>metric3 OR metric1,metric2,metric3 OR metric1,metric2,metric3
     * Each metric: type[:subtype[:selector]]
     * Keeps empty metrics (JMeter's trailing tab trick for rx/tx pairs).
     */
    private List<String> parseMetricsList(String param) {
        List<String> result = new ArrayList<>();
        // Use -1 limit to preserve trailing empty strings (JMeter's trailing tab trick)
        String[] metrics = param.contains(",") ? param.split(",", -1) : param.split("\\t", -1);
        for (String metric : metrics) {
            metric = metric.trim();
            result.add(metric);  // Keep empty metrics for JMeter compatibility
        }
        return result;
    }

    public boolean isShutdownRequested() {
        return shutdownRequested.get();
    }

    public void setShutdownRequested(boolean value) {
        shutdownRequested.set(value);
    }

    public int getClientCount() {
        return clients.size();
    }

    public MetricsStore getMetricsStore() {
        return metricsStore;
    }

    /**
     * Process a metric command for UDP (no queue needed - returns single response).
     * Handles metrics: commands directly using collectMetricAsString.
     */
    public String processMetricForUdp(String metricsParam) {
        List<String> metricList = parseMetricsList(metricsParam);

        StringBuilder result = new StringBuilder();
        for (int i = 0; i < metricList.size(); i++) {
            String metric = metricList.get(i);
            if (isDiskMetric(metric)) {
                if (result.length() > 0) result.append("\t");
                result.append(String.format("%.6f", collector.collectDiskIo(metric)));
            } else if (isNetworkMetric(metric)) {
                String netValue = collectNetworkSingleValue(metric);
                if (result.length() > 0) result.append("\t");
                result.append(netValue);
            } else {
                double value = collector.collectMetric(metric);
                if (result.length() > 0) result.append("\t");
                result.append(String.format("%.6f", value));
            }
        }

        return result.toString() + "\n";
    }
}
