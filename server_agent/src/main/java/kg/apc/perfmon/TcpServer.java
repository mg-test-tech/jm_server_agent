package kg.apc.perfmon;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.*;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TCP server for the PerfMon Server Agent.
 * Listens for connections, dispatches commands to ProtocolHandler.
 */
public class TcpServer implements Runnable {

    private static final Logger log = LogManager.getLogger(TcpServer.class);

    private final int port;
    private ProtocolHandler handler;
    private final MetricsStore metricsStore;
    private final boolean autoShutdown;
    private final AtomicBoolean running;
    private ServerSocket serverSocket;

    public TcpServer(int port, ProtocolHandler handler, MetricsStore metricsStore,
                     boolean autoShutdown, AtomicBoolean running) {
        this.port = port;
        this.handler = handler;
        this.metricsStore = metricsStore;
        this.autoShutdown = autoShutdown;
        this.running = running;
    }

    public void setHandler(ProtocolHandler handler) {
        this.handler = handler;
    }

    public void setActiveMetrics(String clientId, ClientMetrics cm) {
        // Find the ClientThread for this clientId and set its active metrics
        // We store a map of clientId -> ClientMetrics for this purpose
        if (!activeMetricsMap.containsKey(clientId)) {
            activeMetricsMap.put(clientId, cm);
        }
    }

    public void setClientWriter(String clientId, PrintWriter out) {
        clientWriters.put(clientId, out);
    }

    public void writeInitialMetrics(String clientId, String values) {
        PrintWriter out = clientWriters.get(clientId);
        if (out != null) {
            out.print(values + "\n");
            out.flush();
        }
    }

    private final ConcurrentHashMap<String, ClientMetrics> activeMetricsMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, PrintWriter> clientWriters = new ConcurrentHashMap<>();

    public void start() {
        new Thread(this, "tcp-server").start();
    }

    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(port));

            log.info("TCP server listening on port {}", port);

            while (running.get() && !serverSocket.isClosed()) {
                try {
                    Socket client = serverSocket.accept();
                    client.setSoTimeout(0); // No timeout — persistent connection
                    log.info("Accepting new TCP connection from {}", client.getRemoteSocketAddress());

                    String clientId = UUID.randomUUID().toString();
                    ClientThread clientThread = new ClientThread(client, clientId, handler, metricsStore, autoShutdown);
                    // Register the queue before starting the client thread
                    handler.registerClientQueue(clientId, clientThread.getSharedQueue());
                    new Thread(clientThread, "tcp-client-" + clientId).start();

                } catch (SocketException e) {
                    if (running.get()) {
                        log.error("TCP server error: {}", e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            if (running.get()) {
                log.error("TCP server failed: {}", e.getMessage());
            }
        }
    }

    /**
     * Per-client connection thread.
     * Reads commands, sends responses and periodic metrics on the same socket.
     */
    private class ClientThread implements Runnable {

        private final Socket socket;
        private final String clientId;
        private final ProtocolHandler handler;
        private final MetricsStore metricsStore;
        private final boolean autoShutdown;
        private final LinkedBlockingQueue<String> sharedQueue = new LinkedBlockingQueue<>();
        private BufferedReader in;
        private PrintWriter out;
        private ScheduledFuture<?> metricsWriterTask;

        ClientThread(Socket socket, String clientId, ProtocolHandler handler,
                     MetricsStore metricsStore, boolean autoShutdown) {
            this.socket = socket;
            this.clientId = clientId;
            this.handler = handler;
            this.metricsStore = metricsStore;
            this.autoShutdown = autoShutdown;
        }

        public LinkedBlockingQueue<String> getSharedQueue() {
            return sharedQueue;
        }

        @Override
        public void run() {
            boolean exitReceived = false;
            try {
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                out = new PrintWriter(new BufferedOutputStream(socket.getOutputStream()), true);
                handler.tcpServer.setClientWriter(clientId, out);

                // Start metrics writer that drains the shared queue to the socket
                metricsWriterTask = Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "metrics-writer-" + clientId);
                    t.setDaemon(true);
                    return t;
                }).scheduleAtFixedRate(() -> {
                    try {
                        String metricsValue = sharedQueue.poll();
                        if (metricsValue != null) {
                            out.print(metricsValue + "\n");
                            out.flush();
                        }
                    } catch (Exception e) {
                        // Socket may be closed
                    }
                }, 0, 100, TimeUnit.MILLISECONDS);

                String line;
                while ((line = in.readLine()) != null) {
                    String response = handler.processCommand(clientId, line);

                    if (response == null) {
                        // exit command — stop reading commands, but socket stays open
                        // for periodic metrics writing from the scheduled task
                        log.info("Client {} sent exit (metrics keep running)", clientId);
                        exitReceived = true;
                        break;
                    }

                    out.print(response);
                    out.flush();
                }

                if (exitReceived) {
                    log.info("Client {} sent exit, socket kept open for metrics writing", clientId);
                    // Keep thread alive reading from socket so metrics writer daemon keeps running.
                    // When client closes the socket, in.readLine() returns null and we exit.
                    while ((line = in.readLine()) != null) {
                        String response = handler.processCommand(clientId, line);
                        if (response != null) {
                            out.print(response);
                            out.flush();
                        }
                    }
                    log.info("TCP client {} disconnected after exit", clientId);
                } else {
                    log.info("TCP client {} disconnected", clientId);
                    socket.close();
                }

            } catch (SocketException e) {
                log.debug("TCP client {} socket closed: {}", clientId, e.getMessage());
            } catch (IOException e) {
                log.debug("TCP client {} IO error: {}", clientId, e.getMessage());
            } finally {
                // Stop metrics polling only when socket actually closes (not on exit)
                ClientMetrics cm = activeMetricsMap.remove(clientId);
                if (cm != null && !exitReceived) {
                    log.info("Stopping metrics polling for client {} (socket closed)", clientId);
                    metricsStore.stopMetrics(cm);
                } else if (cm != null) {
                    log.info("Metrics polling continues for client {} (exit received, socket open)", clientId);
                }
                if (metricsWriterTask != null) {
                    metricsWriterTask.cancel(false);
                }

                // Auto-shutdown if last client disconnected
                if (autoShutdown) {
                    try {
                        Thread.sleep(5000); // Wait 5 seconds for reconnection
                        if (handler.getClientCount() == 0 && !handler.isShutdownRequested()) {
                            log.info("No clients remaining after grace period — shutting down");
                            running.set(false);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }
}
