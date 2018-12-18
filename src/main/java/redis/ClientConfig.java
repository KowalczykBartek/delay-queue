package redis;

public class ClientConfig {
    private final int maxConcurrentRequests;

    public ClientConfig()
    {
        this(1024);
    }

    public ClientConfig(final int maxConcurrentRequests) {
        this.maxConcurrentRequests = maxConcurrentRequests;
    }

    public int getMaxConcurrentRequests() {
        return maxConcurrentRequests;
    }
}
