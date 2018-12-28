package redis;

public class ClientConfig {
    private final int maxConcurrentRequests;
    private final int port;
    private final String host;

    public ClientConfig() {
        this(1024,  6379, "127.0.0.1");
    }

    public ClientConfig(final int maxConcurrentRequests, final int port, final String host) {
        this.maxConcurrentRequests = maxConcurrentRequests;
        this.port = port;
        this.host = host;
    }

    public int getMaxConcurrentRequests() {
        return maxConcurrentRequests;
    }

    public int getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }
}
