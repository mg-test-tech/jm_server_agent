package kg.apc.perfmon;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;

/**
 * UDP server for the PerfMon Server Agent.
 * Receives metrics-single commands and responds with values.
 */
public class UdpServer {

    private static final Logger log = LogManager.getLogger(UdpServer.class);

    private final int port;
    private final ProtocolHandler handler;
    private DatagramSocket socket;
    private volatile boolean running = false;

    public UdpServer(int port, ProtocolHandler handler) {
        this.port = port;
        this.handler = handler;
    }

    public void start() {
        new Thread(() -> {
            running = true;
            try {
                socket = new DatagramSocket(port);
                socket.setSoTimeout(1000); // 1 second timeout for polling

                log.info("UDP server listening on port {}", port);

                byte[] buffer = new byte[65536];

                while (running && socket != null && !socket.isClosed()) {
                    try {
                        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                        socket.receive(packet);

                        String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8).trim();
                        String clientId = packet.getAddress().getHostAddress() + ":" + packet.getPort();

                        if (message.isEmpty()) continue;

                        log.debug("UDP command from {}: {}}", clientId, message);

                        String response = handler.processCommand(clientId, message);
                        if (response == null || response.isEmpty() || response.contains("Error:")) {
                            // For UDP, handle metrics commands directly (no queue for UDP)
                            if (message.trim().startsWith("metrics:") && !message.trim().startsWith("metrics-single:")) {
                                String metricsParam = message.trim().substring("metrics:".length());
                                String directResponse = handler.processMetricForUdp(metricsParam);
                                if (directResponse != null && !directResponse.isEmpty()) {
                                    response = directResponse;
                                } else if (!response.contains("Error:")) {
                                    response = "0.000000\n";
                                }
                            } else if (response.isEmpty()) {
                                continue;
                            }
                        }
                        if (response != null && !response.isEmpty()) {
                            byte[] responseData = response.getBytes(StandardCharsets.UTF_8);
                            DatagramPacket reply = new DatagramPacket(
                                responseData, responseData.length,
                                packet.getAddress(), packet.getPort()
                            );
                            socket.send(reply);
                        }

                    } catch (SocketTimeoutException e) {
                        // Normal timeout — keep listening
                    } catch (IOException e) {
                        if (socket != null && !socket.isClosed()) {
                            log.error("UDP server error: {}", e.getMessage());
                        }
                    }
                }

            } catch (BindException e) {
                log.error("UDP port {} already in use: {}", port, e.getMessage());
                System.err.println("Error: UDP port " + port + " already in use.");
            } catch (IOException e) {
                log.error("UDP server IO error: {}", e.getMessage());
            }
        }, "udp-server").start();
    }

    public void stop() {
        running = false;
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }
}
