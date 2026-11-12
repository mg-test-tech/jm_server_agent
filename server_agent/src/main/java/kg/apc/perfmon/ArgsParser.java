package kg.apc.perfmon;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Parses command-line arguments for the Server Agent.
 * 
 * Arguments:
 *   --tcp-port PORT      TCP listening port (default 4444, 0 to disable)
 *   --udp-port PORT      UDP listening port (default 4444, 0 to disable)
 *   --interval SECONDS   Metrics collection interval in seconds (default 1)
 *   --no-exec            Disable exec and tail metric types
 *   --auto-shutdown      Auto-shutdown when last client disconnects (TCP only)
 *   --sysinfo            Print available system objects and exit
 *   --loglevel LEVEL     Logging level: DEBUG, INFO, WARN, ERROR
 */
public class ArgsParser {

    private int tcpPort = 4444;
    private int udpPort = 4444;
    private int interval = 1;
    private boolean noExec = false;
    private boolean autoShutdown = false;
    private boolean sysinfo = false;
    private String loglevel = "INFO";

    private static final Logger log = LogManager.getLogger(ArgsParser.class);

    public ArgsParser(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--tcp-port":
                    if (i + 1 < args.length) {
                        tcpPort = parseInt(args[++i], 4444);
                    }
                    break;
                case "--udp-port":
                    if (i + 1 < args.length) {
                        udpPort = parseInt(args[++i], 4444);
                    }
                    break;
                case "--interval":
                    if (i + 1 < args.length) {
                        interval = parseInt(args[++i], 1);
                    }
                    break;
                case "--no-exec":
                    noExec = true;
                    break;
                case "--auto-shutdown":
                    autoShutdown = true;
                    break;
                case "--sysinfo":
                    sysinfo = true;
                    break;
                case "--loglevel":
                    if (i + 1 < args.length) {
                        loglevel = args[++i].toUpperCase();
                    }
                    break;
                default:
                    log.warn("Unknown argument: {}", arg);
                    break;
            }
        }

        log.info("TCP port: {}, UDP port: {}, interval: {}s, no-exec: {}, auto-shutdown: {}, sysinfo: {}",
            tcpPort, udpPort, interval, noExec, autoShutdown, sysinfo);
    }

    public int getTcpPort() { return tcpPort; }
    public int getUdpPort() { return udpPort; }
    public int getInterval() { return interval; }
    public boolean isNoExec() { return noExec; }
    public boolean isAutoShutdown() { return autoShutdown; }
    public boolean isSysinfo() { return sysinfo; }
    public String getLogLevel() { return loglevel; }

    private int parseInt(String value, int defaultValue) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            log.warn("Invalid integer '{}', using default {}", value, defaultValue);
            return defaultValue;
        }
    }
}
