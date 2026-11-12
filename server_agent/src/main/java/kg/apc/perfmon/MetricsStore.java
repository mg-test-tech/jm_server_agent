package kg.apc.perfmon;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages continuous metrics collection for running 'metrics' commands.
 * Polls at a configurable interval and stores pending metrics for clients.
 */
public class MetricsStore {

    private static final Logger log = LogManager.getLogger(MetricsStore.class);

    private volatile int intervalSeconds = 1;
    private final ConcurrentHashMap<ClientMetrics, ScheduledFuture<?>> runningTasks = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(
        Runtime.getRuntime().availableProcessors(),
        r -> {
            Thread t = new Thread(r, "metrics-poller");
            t.setDaemon(true);
            return t;
        }
    );

    public MetricsStore(int initialInterval) {
        this.intervalSeconds = Math.max(1, initialInterval);
    }

    public MetricsStore() {
        this.intervalSeconds = 1;
    }

    public void setInterval(int seconds) {
        this.intervalSeconds = Math.max(1, seconds);
        log.info("Global metrics interval set to {}s", this.intervalSeconds);

        // Restart all running metric sessions with new interval
        for (ClientMetrics cm : runningTasks.keySet()) {
            stopMetrics(cm);
            startMetrics(cm);
        }
    }

    public int getInterval() {
        return intervalSeconds;
    }

    /**
     * Start continuous metrics collection for a client.
     */
    public void startMetrics(ClientMetrics client) {
        if (runningTasks.containsKey(client)) {
            log.debug("Metrics already running for {}", client);
            return;
        }

        log.info("Starting metrics polling for {} at {}s interval", client, intervalSeconds);

        // Synchronize warm-up and scheduler to prevent race condition
        java.util.concurrent.CountDownLatch warmUpDone = new java.util.concurrent.CountDownLatch(1);

        // Send warm-up poll in a separate thread
        Thread warmUpThread = new Thread(() -> {
            warmUpPoll(client);
            warmUpDone.countDown();
        }, "warm-up-poll");
        warmUpThread.start();

        // Wait for warm-up to complete before scheduling
        try {
            warmUpDone.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Track if this is the first scheduled poll (should be skipped)
        final boolean[] firstPoll = {true};

        ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(() -> {
            try {
                // Skip first scheduled poll (warm-up already queued)
                if (firstPoll[0]) {
                    firstPoll[0] = false;
                    log.info("Skipping first scheduled poll, warm-up already queued");
                    return;
                }
                collectAndStore(client);
            } catch (Exception e) {
                log.debug("Error polling metrics for {}: {}", client, e.getMessage());
            }
        }, 0, intervalSeconds, TimeUnit.SECONDS);

        runningTasks.put(client, future);
        log.info("Started metrics polling for {} at {}s interval", client, intervalSeconds);
    }

    /**
     * Warm-up poll: collect metrics using single-value methods to match
     * what collectAndStore returns (after pipe expansion).
     */
    private void warmUpPoll(ClientMetrics client) {
        // Clear any stale values from previous metrics session
        String clientId = client.getClientId();
        // Drain any pending values from the shared queue
        int drained = 0;
        String item;
        while ((item = client.pollResult()) != null && drained < 100) {
            drained++;
        }
        // Always clear latestResult to prevent stale data leak
        client.getLatestResult().set("");
        if (drained > 0) {
            log.info("Drained {} stale values from queue for client {}", drained, clientId);
        }

        MetricCollector collector = client.getCollector();
        List<String> metrics = client.getMetrics();

        StringBuilder values = new StringBuilder();
        for (int i = 0; i < metrics.size(); i++) {
            String metric = metrics.get(i);
            String[] parts = metric.split(":", 3);
            String subtype = parts.length >= 2 ? parts[1].toLowerCase() : "";
            String selector = parts.length >= 3 ? parts[2] : "";

            // Use single-value methods for network metrics to match collectAndStore output
            // Strip spaces and slashes: JMeter sends "disks i/o" / "network i/o"
            String typeNorm = parts[0].trim().toLowerCase().replace(" ", "").replace("/", "");

            // Disk I/O metrics: single delta value per poll (warm-up establishes
            // the baseline so the first scheduled poll returns a real delta)
            if (typeNorm.equals("disks") || typeNorm.equals("disk") || typeNorm.equals("diskio")
                || typeNorm.equals("disksio")) {
                values.append(String.format("%.6f", collector.collectDiskIo(metric)));
                if (i < metrics.size() - 1) values.append("\t");
                continue;
            }

            boolean isNetwork = typeNorm.equals("network");
            // Track context: bare tx after network: metric should be treated as network:tx
            boolean prevWasNetwork = (i > 0) && metrics.get(i - 1).toLowerCase().replace(" ", "").replace("/", "").startsWith("network:");
            // Bare rx/tx (no subtype, no network: prefix) returns pair
            boolean isBareRx = !isNetwork && "rx".equalsIgnoreCase(parts[0]) && subtype.isEmpty();
            boolean isBareTx = !isNetwork && "tx".equalsIgnoreCase(parts[0]) && subtype.isEmpty() && !prevWasNetwork;

            if (isNetwork || isBareRx || isBareTx) {
                // Determine the actual metric name for rx/tx handling
                String metricName = isNetwork ? subtype : parts[0];
                boolean isRx = metricName.equals("rx") || metricName.equals("bytesrecv") || metricName.equals("recvbytes") || metricName.equals("bytesrx") || metricName.equals("rxbytes");
                boolean isTx = metricName.equals("tx") || metricName.equals("bytessent") || metricName.equals("sentbytes") || metricName.equals("bytestx") || metricName.equals("txbytes");
                
                // Bare rx returns TWO values only when it's the only metric (standalone);
                // returns SINGLE value when followed by another metric (matches original PerfMon agent)
                // Bare tx ALWAYS returns SINGLE value (original agent treats rx/tx as separate single-value metrics)
                if (isRx && isBareRx) {
                    int remainingNonEmpty = 0;
                    for (int j = i + 1; j < metrics.size(); j++) {
                        if (!metrics.get(j).isEmpty()) {
                            remainingNonEmpty++;
                        }
                    }
                    if (remainingNonEmpty == 0) {
                        // Only metric: return TWO values: rx_value\ttx_value (matches original PerfMon agent)
                        double rx = collector.collectNetworkRx(selector);
                        double tx = collector.collectNetworkTx(selector);
                        log.info("Warm-up: calling collectNetworkRx={}, collectNetworkTx={}", rx, tx);
                        values.append(String.format("%.6f", rx)).append("\t").append(String.format("%.6f", tx));
                    } else {
                        // Followed by other metrics: return SINGLE value
                        values.append(String.format("%.6f", collector.collectNetworkRx(selector)));
                    }
                } else if (isTx && isBareTx) {
                    // tx always returns single value
                    values.append(String.format("%.6f", collector.collectNetworkTx(selector)));
                } else if (isRx || isTx) {
                    // network:rx or network:tx: return SINGLE value
                    if (isRx) {
                        values.append(String.format("%.6f", collector.collectNetworkRx(selector)));
                    } else {
                        values.append(String.format("%.6f", collector.collectNetworkTx(selector)));
                    }
                } else if (subtype.equals("used") || subtype.isEmpty()) {
                    // "used" or empty subtype (bare "network i/o:"): combined rx+tx
                    values.append(String.format("%.6f", collector.collectNetworkUsed(selector)));
                } else if (subtype.equals("speed")) {
                    values.append(collector.collectNetworkSpeed(selector));
                } else if (subtype.equals("queue")) {
                    values.append("0.000000");
                } else if (subtype.equals("rxpackets") || subtype.equals("packetsrecv")) {
                    values.append(collector.collectNetworkPacketsRx(selector));
                } else if (subtype.equals("txpackets") || subtype.equals("packetssent")) {
                    values.append(collector.collectNetworkPacketsTx(selector));
                } else if (subtype.equals("errors") || subtype.equals("rxerr")) {
                    values.append(collector.collectNetworkErrors(selector));
                } else if (subtype.equals("rxdrops") || subtype.equals("drops")) {
                    values.append(collector.collectNetworkDrops(selector));
                } else {
                    values.append(collector.collectMetricAsString(metric));
                }
            } else {
                // Skip empty metric tokens (JMeter's trailing tab placeholder)
                if (metric.isEmpty()) continue;
                values.append(collector.collectMetricAsString(metric));
            }

            // Only append tab separator for non-empty metrics (avoids trailing tab after empty token)
            if (!metric.isEmpty() && i < metrics.size() - 1) {
                values.append("\t");
            }

        }

        log.info("Warm-up: values={}", values.toString());
        if (values.length() > 0) {
            client.storeResult(values.toString());
            log.info("Warm-up: stored result, queue size={}", client.getQueueSize());
        }
    }

    /**
     * Stop continuous metrics collection for a client.
     */
    public void stopMetrics(ClientMetrics client) {
        ScheduledFuture<?> future = runningTasks.remove(client);
        if (future != null) {
            future.cancel(false);
            log.debug("Stopped metrics polling for {}", client);
        }
    }

    private void collectAndStore(ClientMetrics client) {
        MetricCollector collector = client.getCollector();
        List<String> metrics = client.getMetrics();

        StringBuilder values = new StringBuilder();
        for (int i = 0; i < metrics.size(); i++) {
            String metric = metrics.get(i);
            String[] parts = metric.split(":", 3);
            String subtype = parts.length >= 2 ? parts[1].toLowerCase() : "";
            String selector = parts.length >= 3 ? parts[2] : "";

            // Strip spaces and slashes: JMeter sends "disks i/o" / "network i/o"
            String typeNorm = parts[0].trim().toLowerCase().replace(" ", "").replace("/", "");

            // Disk I/O metrics: single delta value per poll
            if (typeNorm.equals("disks") || typeNorm.equals("disk") || typeNorm.equals("diskio")
                || typeNorm.equals("disksio")) {
                values.append(String.format("%.6f", collector.collectDiskIo(metric)));
                if (i < metrics.size() - 1) values.append("\t");
                continue;
            }

            // Use single-value methods for network metrics to match warm-up output
            // Strip spaces: JMeter sends "network i/o" (with spaces)
            boolean isNetwork = typeNorm.equals("network");
            // Track context: bare tx after network: metric should be treated as network:tx
            boolean prevWasNetwork = (i > 0) && metrics.get(i - 1).toLowerCase().replace(" ", "").startsWith("network:");
            // Bare rx/tx (no subtype, no network: prefix) returns pair
            boolean isBareRx = !isNetwork && "rx".equalsIgnoreCase(parts[0]) && subtype.isEmpty();
            boolean isBareTx = !isNetwork && "tx".equalsIgnoreCase(parts[0]) && subtype.isEmpty() && !prevWasNetwork;
            if (isNetwork || isBareRx || isBareTx) {
                // Determine the actual metric name for rx/tx handling
                String metricName = isNetwork ? subtype : parts[0];
                boolean isRx = metricName.equals("rx") || metricName.equals("bytesrecv") || metricName.equals("recvbytes") || metricName.equals("bytesrx") || metricName.equals("rxbytes");
                boolean isTx = metricName.equals("tx") || metricName.equals("bytessent") || metricName.equals("sentbytes") || metricName.equals("bytestx") || metricName.equals("txbytes");
                
                // Bare rx returns TWO values only when it's the only metric (standalone);
                // returns SINGLE value when followed by another metric (matches original PerfMon agent)
                // Bare tx ALWAYS returns SINGLE value (original agent treats rx/tx as separate single-value metrics)
                if (isRx && isBareRx) {
                    int remainingNonEmpty = 0;
                    for (int j = i + 1; j < metrics.size(); j++) {
                        if (!metrics.get(j).isEmpty()) {
                            remainingNonEmpty++;
                        }
                    }
                    if (remainingNonEmpty == 0) {
                        // Only metric: return TWO values: rx_value\ttx_value
                        double rx = collector.collectNetworkRx(selector);
                        double tx = collector.collectNetworkTx(selector);
                        values.append(String.format("%.6f", rx)).append("\t").append(String.format("%.6f", tx));
                    } else {
                        values.append(String.format("%.6f", collector.collectNetworkRx(selector)));
                    }
                } else if (isTx && isBareTx) {
                    // tx always returns single value
                    values.append(String.format("%.6f", collector.collectNetworkTx(selector)));
                } else if (isRx || isTx) {
                    // network:rx or network:tx: return SINGLE value
                    if (isRx) {
                        values.append(String.format("%.6f", collector.collectNetworkRx(selector)));
                    } else {
                        values.append(String.format("%.6f", collector.collectNetworkTx(selector)));
                    }
                } else if (subtype.equals("used") || subtype.isEmpty()) {
                    // "used" or empty subtype (bare "network i/o:"): combined rx+tx
                    values.append(String.format("%.6f", collector.collectNetworkUsed(selector)));
                } else if (subtype.equals("speed")) {
                    values.append(collector.collectNetworkSpeed(selector));
                } else if (subtype.equals("queue")) {
                    values.append("0.000000");
                } else if (subtype.equals("rxpackets") || subtype.equals("packetsrecv")) {
                    values.append(collector.collectNetworkPacketsRx(selector));
                } else if (subtype.equals("txpackets") || subtype.equals("packetssent")) {
                    values.append(collector.collectNetworkPacketsTx(selector));
                } else if (subtype.equals("errors") || subtype.equals("rxerr")) {
                    values.append(collector.collectNetworkErrors(selector));
                } else if (subtype.equals("rxdrops") || subtype.equals("drops")) {
                    values.append(collector.collectNetworkDrops(selector));
                } else {
                    values.append(collector.collectMetricAsString(metric));
                }
            } else {
                // Skip empty metric tokens (JMeter's trailing tab placeholder)
                if (metric.isEmpty()) continue;
                values.append(collector.collectMetricAsString(metric));
            }

            // Only append tab separator for non-empty metrics (avoids trailing tab after empty token)
            if (!metric.isEmpty() && i < metrics.size() - 1) {
                values.append("\t");
            }
        }

        log.info("collectAndStore: values={}, queue size before={}", values, client.getQueueSize());
        client.storeResult(values.toString());
        log.info("collectAndStore: stored, queue size after={}", client.getQueueSize());
    }

    /**
     * Shutdown all tasks.
     */
    public void shutdown() {
        scheduler.shutdownNow();
    }

    /**
     * Get count of running metric sessions.
     */
    public int getActiveSessions() {
        return runningTasks.size();
    }
}
