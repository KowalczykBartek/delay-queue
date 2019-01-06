package redis;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.redis.InlineCommandRedisMessage;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.parser.RedisMessageParser;

import java.util.concurrent.CompletableFuture;

/**
 * Redis client maintaining single, persistent connection but register in one of shared threads (SHARED_EVENT_LOOP_GROUP)
 */
public class RedisClient {
    private static final Logger LOG = LoggerFactory.getLogger(RedisClient.class);
    private static final EventLoopGroup SHARED_EVENT_LOOP_GROUP = new NioEventLoopGroup(4);
    private final ChannelFuture channelFuture;
    private final Bootstrap bootstrap;
    private volatile Channel channel;
    private final RedisMessageParser parser;

    public RedisClient() {
        final ClientConfig clientConfig = new ClientConfig();

        parser = new RedisMessageParser(clientConfig.getMaxConcurrentRequests());

        bootstrap = new Bootstrap();
        bootstrap.group(SHARED_EVENT_LOOP_GROUP).channel(NioSocketChannel.class)//
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(final SocketChannel ch) {
                        final ChannelPipeline pipeline = ch.pipeline();
                        //pipeline.addLast(new LoggingHandler(LogLevel.INFO));
                        pipeline.addLast(new RedisEncoder());
                        pipeline.addLast(new RedisDecoder());
                        pipeline.addLast(parser);
                    }
                });

        channelFuture = bootstrap.connect(clientConfig.getHost(), clientConfig.getPort());
        channel = channelFuture.channel();
    }

    public void awaitConnection() throws InterruptedException {
        channelFuture.await();
    }

    /**
     * Perform HSET operation and return value according to REDIS protocol.
     *
     * @param resource
     * @param messageId
     * @param message
     * @return CompletableFuture representing query result. Always completed on socket's loop, so you should
     * never ever block it ! but you can gain because of TPC architecture.
     */
    public CompletableFuture<Long> hset(final String resource, final String messageId, final String message) {
        final String hsetQuery = "HSET " + resource + " " + messageId + " \"" + message + "\"";
        return query(hsetQuery)
                .thenApply(hsetResult -> (Long) hsetResult);
    }

    /**
     * Perform ZADD operation and return value according to REDIS protocol.
     *
     * @param resource
     * @param when
     * @param messageId
     * @return CompletableFuture representing query result. Always completed on socket's loop, so you should
     * never ever block it ! but you can gain because of TPC architecture.
     */
    public CompletableFuture<Long> zadd(final String resource, final long when, final String messageId) {
        final String zaddQuery = "ZADD " + resource + " " + when + " \"" + messageId + "\"";
        return query(zaddQuery)
                .thenApply(hsetResult -> (Long) hsetResult);
    }

    /**
     * Perform ZRANGEBYSCORE operation and return value according to REDIS protocol.
     *
     * @param resource
     * @param now
     * @return CompletableFuture representing query result. Always completed on socket's loop, so you should
     * never ever block it ! but you can gain because of TPC architecture.
     */
    public CompletableFuture<String[]> zrangeByScore(final String resource, final long now) {
        final String zrangeByScoreQuery = "ZRANGEBYSCORE " + resource + " 0 " + now + " WITHSCORES";
        return query(zrangeByScoreQuery)
                .thenApply(object -> (String[]) object);
    }

    /**
     * Perform ZREM operation and return value according to REDIS protocol.
     *
     * @param resource
     * @param messageId
     * @return CompletableFuture representing query result. Always completed on socket's loop, so you should
     * never ever block it ! but you can gain because of TPC architecture.
     */
    public CompletableFuture<Long> zrem(final String resource, final String messageId) {
        final String zremnQuery = "ZREM " + resource + " \"" + messageId + "\"";
        return query(zremnQuery)
                .thenApply(object -> (Long) object);
    }

    /**
     * Perform HGET operation and return value according to REDIS protocol.
     *
     * @param resource
     * @param messageId
     * @return CompletableFuture representing query result. Always completed on socket's loop, so you should
     * never ever block it ! but you can gain because of TPC architecture.
     */
    public CompletableFuture<String> hget(final String resource, final String messageId) {
        final String hgetQuery = "HGET " + resource + " \"" + messageId + "\"";
        return query(hgetQuery)
                .thenApply(hgetResult -> (String) hgetResult);
    }

    /**
     * Perform HDEL operation and return value according to REDIS protocol.
     *
     * @param resource
     * @param messageId
     * @return CompletableFuture representing query result. Always completed on socket's loop, so you should
     * never ever block it ! but you can gain because of TPC architecture.
     */
    public CompletableFuture<Long> hdel(final String resource, final String messageId) {
        final String hdelQuery = "HDEL " + resource + " \"" + messageId + "\"";
        return query(hdelQuery)
                .thenApply(object -> (Long) object);
    }

    public CompletableFuture<Object> query(final String query) {
        final CompletableFuture<Object> completableFuture = new CompletableFuture<>();

        /*
         * To prevent order and "atomicity".
         */
        if (channel.eventLoop().inEventLoop()) {
            addAndWrite(completableFuture, query);
        } else {
            channel.eventLoop().execute(() -> {
                addAndWrite(completableFuture, query);
            });
        }

        return completableFuture;
    }

    public EventLoop getLoop() {
        return channel.eventLoop();
    }

    private void addAndWrite(final CompletableFuture<Object> completableFuture, final String query) {

        /*
         * I will not implement back-pressure at the moment - will just throw an exception.
         */
        if (!channel.isWritable()) {
            completableFuture.completeExceptionally(new RuntimeException("Channel not writable !"));
        }

        final InlineCommandRedisMessage command = new InlineCommandRedisMessage(query);
        if (!parser.registerCallback(completableFuture)) {
            completableFuture.completeExceptionally(new RuntimeException("Too much concurrent requests !"));
        } else {
            channel.write(command);
            channel.flush();
        }
    }

}
