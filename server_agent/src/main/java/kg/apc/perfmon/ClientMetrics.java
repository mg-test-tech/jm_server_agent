package kg.apc.perfmon;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Per-client session state for the PerfMon Server Agent.
 * Tracks metric list, polling results, and connection state.
 */
public class ClientMetrics {

    private final MetricCollector collector;
    private final List<String> metrics;
    private final Queue<String> resultQueue;
    private final AtomicReference<String> latestResult = new AtomicReference<>("");
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final String clientId;

    public ClientMetrics(String clientId, MetricCollector collector, List<String> metrics, Queue<String> sharedQueue) {
        this.clientId = clientId;
        this.collector = collector;
        this.metrics = metrics;
        this.resultQueue = sharedQueue != null ? sharedQueue : new ConcurrentLinkedQueue<>();
    }

    public MetricCollector getCollector() { return collector; }
    public List<String> getMetrics() { return metrics; }
    public String getClientId() { return clientId; }

    public void storeResult(String values) {
        latestResult.set(values);
        resultQueue.offer(values);
        // Keep queue bounded
        while (resultQueue.size() > 100) {
            resultQueue.poll();
        }
    }

    /**
     * Get latest result. Returns immediately.
     * In continuous mode, the latest poll result is always available.
     */
    public String pollResult() {
        String result = resultQueue.poll();
        return result != null ? result : latestResult.get();
    }

    public AtomicBoolean getRunning() { return running; }
    public int getQueueSize() { return resultQueue.size(); }
    public java.util.concurrent.atomic.AtomicReference<String> getLatestResult() { return latestResult; }

    @Override
    public String toString() {
        return "ClientMetrics{id=" + clientId + ", metrics=" + metrics + ", running=" + running.get() + "}";
    }
}
