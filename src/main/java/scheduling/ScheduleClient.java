package scheduling;

import io.netty.channel.EventLoopGroup;
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
                    final String zaddQuery = "ZADD queue " + when + " \"" + messageId + "\"";
                    redisClient.query(zaddQuery)
                            .thenAccept(zaddResult -> {
                                scheduleResult.complete(true);
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

    public CompletableFuture<Object> queryScheduledMessages(final long now) {
        final CompletableFuture<Object> queryResult = new CompletableFuture<>();

        //damm, this is unlimited :/
        redisClient.query("ZRANGEBYSCORE queue 0 " + now + " WITHSCORES")
                .thenAccept(object -> queryResult.complete(object));

        return queryResult;
    }

    public CompletableFuture<Object> popMessage(final long originalScheduleTime, final String messageId) {
        final CompletableFuture<Object> popResult = new CompletableFuture<>();

        final String zaddQuery = "ZADD unacks " + originalScheduleTime + " \"" + messageId + "\"";
        redisClient.query(zaddQuery)
                .thenCompose(result -> {
                    final String zremnQuery = "ZREM queue \"" + messageId + "\"";
                    return redisClient.query(zremnQuery);
                })
                .thenCompose(zremnQuery -> {
                    final String hgetQuery = "HGET messages \"" + messageId + "\"";
                    return redisClient.query(hgetQuery)
                            .thenAccept(hgetResult -> {
                                popResult.complete(hgetResult);
                            });
                })
                .exceptionally(throwable -> {
                    popResult.completeExceptionally(throwable);
                    return null;
                });

        return popResult;
    }

    public CompletableFuture<Object> ackMessage(final String messageId) {
        final CompletableFuture<Object> ackResult = new CompletableFuture<>();

        final String zremQuery = "ZREM unacks " + " \"" + messageId + "\"";
        redisClient.query(zremQuery)
                .thenCompose(result -> {
                    final String hdelQuery = "HDEL messages \"" + messageId + "\"";
                    return redisClient.query(hdelQuery)
                            .thenAccept(hdelResult -> {
                                ackResult.complete(hdelResult);
                            });
                })
                .exceptionally(throwable -> {
                    ackResult.completeExceptionally(throwable);
                    return null;
                });

        return ackResult;
    }

    public EventLoopGroup getLoop()
    {
        return redisClient.getLoop();
    }

}
