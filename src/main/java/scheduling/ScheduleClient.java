package scheduling;

import io.netty.channel.EventLoop;
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

    /**
     * TODO docu
     */
    public CompletableFuture<Boolean> scheduleEvent(final long when, final String messageId, final String message) {
        final CompletableFuture<Boolean> scheduleResult = new CompletableFuture<>();

        redisClient.hset("messages", messageId, message)
                .thenAccept(hsetResult -> {
                    redisClient.zadd("queue", when, messageId)
                            .thenAccept(zaddResult -> {
                                scheduleResult.complete(true);
                            });
                })//
                .exceptionally(exception -> {
                    LOG.error("{} {}", messageId, exception);
                    scheduleResult.completeExceptionally(exception);
                    return null;
                });

        return scheduleResult;
    }

    /**
     * TODO docu
     */
    public CompletableFuture<String[]> queryScheduledMessages(final long now) {
        final CompletableFuture<String[]> queryResult = new CompletableFuture<>();

        //todo warn, unlimited query result
        redisClient.zrangeByScore("queue", now)
                .thenAccept(queryResult::complete)//
                .exceptionally(exception -> {
                    LOG.error("{} {}", exception);
                    queryResult.completeExceptionally(exception);
                    return null;
                });

        return queryResult;
    }

    /**
     * TODO docu
     */
    public CompletableFuture<String> popMessage(final long originalScheduleTime, final String messageId) {
        final CompletableFuture<String> popResult = new CompletableFuture<>();

        redisClient.zadd("unacks", originalScheduleTime, messageId)
                .thenCompose(result -> redisClient.zrem("queue", messageId))
                .thenCompose(zremnQuery -> redisClient.hget("messages", messageId))
                .exceptionally(throwable -> {
                    popResult.completeExceptionally(throwable);
                    return null;
                })
                .thenApply(result -> {
                    popResult.complete(result);
                    return null;
                });

        return popResult;
    }

    /**
     * TODO docu
     */
    public CompletableFuture<Object> ackMessage(final String messageId) {
        final CompletableFuture<Object> ackResult = new CompletableFuture<>();

        redisClient.zrem("unacks", messageId)
                .thenCompose(result -> redisClient.hdel("messages", messageId))
                .thenApply(result -> {
                    ackResult.complete(result);
                    return null;
                })
                .exceptionally(throwable -> {
                    ackResult.completeExceptionally(throwable);
                    return null;
                });

        return ackResult;
    }

    public EventLoop getLoop() {
        return redisClient.getLoop();
    }
}
