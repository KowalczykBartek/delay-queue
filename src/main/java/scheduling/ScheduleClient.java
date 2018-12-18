package scheduling;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.RedisClient;

import java.util.concurrent.CompletableFuture;

public class ScheduleClient {
    private static final Logger LOG = LoggerFactory.getLogger(ScheduleClient.class);

    private final RedisClient redisClient;

    public ScheduleClient() {
        redisClient = new RedisClient();
    }

    public void awaitConnection() throws InterruptedException {
        redisClient.awaitConnection();
    }

    public CompletableFuture<Object> scheduleEvent(final long when, final String messageId, final String message) {

        final CompletableFuture<Object> scheduleResult = new CompletableFuture<>();

        final String hsetQuery = "HSET messages " + messageId + " \"" + message + "\"";
        redisClient.query(hsetQuery)
                .thenAccept(hsetResult -> {
                    LOG.info("HSET {} result {}", messageId, hsetResult);

                    final String zaddQuery = "ZADD queue " + when + " \"" + messageId + "\"";
                    redisClient.query(zaddQuery)
                            .thenAccept(zaddResult -> {
                                scheduleResult.complete(true);
                                LOG.info("ZADD {} result {}", messageId, hsetResult);
                            })//
                            .exceptionally(exception -> {
                                LOG.error("{} {}", messageId, exception);
                                scheduleResult.completeExceptionally(exception);
                                return null;
                            });
                })//
                .exceptionally(exception -> {
                    LOG.error("{} {}", messageId, exception);
                    scheduleResult.completeExceptionally(exception);
                    return null;
                });

        return scheduleResult;
    }
}
