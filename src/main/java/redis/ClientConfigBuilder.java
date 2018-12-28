package redis;

public class ClientConfigBuilder {
    public int maxConcurrentRequests;
    public int port;
    public String host;

    public void setMaxConcurrentRequests(final int maxConcurrentRequests, final int openChannels, final int threadsCount, final int port, final String host) {
        this.maxConcurrentRequests = maxConcurrentRequests;
        this.port = port;
        this.host = host;
    }

    public ClientConfig build() {
        return new ClientConfig(maxConcurrentRequests, port, host);
    }
}
