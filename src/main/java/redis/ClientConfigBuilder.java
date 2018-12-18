package redis;

public class ClientConfigBuilder {
    public int maxConcurrentRequests;

    public void setMaxConcurrentRequests(final int maxConcurrentRequests) {
        this.maxConcurrentRequests = maxConcurrentRequests;
    }

    public ClientConfig build()
    {
        return new ClientConfig(maxConcurrentRequests);
    }
}
