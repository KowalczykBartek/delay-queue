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

    public CompletableFuture<Object> queryMessages(final long now) {
        final CompletableFuture<Object> queryResult = new CompletableFuture<>();

        //damm, this is unlimited :/
        redisClient.query("ZRANGE queue 0 " + now)
                .thenAccept(object -> {
                    queryResult.complete(object);
                });

        return queryResult;
    }

    public CompletableFuture<Object> popMessage(final String messageId) {
        final CompletableFuture<Object> popResult = new CompletableFuture<>();

        //FIXME
        final long FIXME = System.currentTimeMillis();

        final String zaddQuery = "ZADD unacks " + FIXME + " \"" + messageId + "\"";
        redisClient.query(zaddQuery)
                .thenCompose(result -> {
                    LOG.info("{} {}", zaddQuery, result);

                    final String zremnQuery = "ZREM queue \"" + messageId + "\"";
                    return redisClient.query(zremnQuery)
                            .thenAccept(zremResult -> LOG.info("{} {}", zremnQuery, result));
                })
                .thenCompose(zremnQuery -> {
                    final String hgetQuery = "HSET messages field1 \"" + messageId + "\"";
                    return redisClient.query(hgetQuery)
                            .thenAccept(hgetResult -> LOG.info("{} {}", hgetQuery, hgetResult));
                });

        return popResult;
    }
}
