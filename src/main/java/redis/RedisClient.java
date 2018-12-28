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

import java.util.concurrent.CompletableFuture;

/**
 * Redis client maintaining single, persistent connection.
 */
public class RedisClient {
    private static final Logger LOG = LoggerFactory.getLogger(RedisClient.class);
    private static final EventLoopGroup SHARED_EVENT_LOOP_GROUP = new NioEventLoopGroup(4);
    private final ChannelFuture channelFuture;
    private final Bootstrap bootstrap;
    private volatile Channel channel;
    private final Dispatcher dispatcher;

    public RedisClient() {
        final ClientConfig clientConfig = new ClientConfig();

        dispatcher = new Dispatcher(clientConfig.getMaxConcurrentRequests());

        bootstrap = new Bootstrap();
        bootstrap.group(SHARED_EVENT_LOOP_GROUP).channel(NioSocketChannel.class)//
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(final SocketChannel ch) {
                        final ChannelPipeline pipeline = ch.pipeline();
                        //pipeline.addLast(new LoggingHandler(LogLevel.INFO));
                        pipeline.addLast(new RedisEncoder());
                        pipeline.addLast(new RedisDecoder());
                        pipeline.addLast(dispatcher);
                    }
                });

        channelFuture = bootstrap.connect(clientConfig.getHost(), clientConfig.getPort());
        channel = channelFuture.channel();
    }

    public void awaitConnection() throws InterruptedException {
        channelFuture.await();
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
         * I will not implement back-pressure - will just throw an exception.
         */
        if (!channel.isWritable()) {
            completableFuture.completeExceptionally(new RuntimeException("Channel not writable !"));
        }

        final InlineCommandRedisMessage command = new InlineCommandRedisMessage(query);
        if (!dispatcher.registerCallback(completableFuture)) {
            completableFuture.completeExceptionally(new RuntimeException("Too much concurrent requests !"));
        } else {
            channel.write(command);
            channel.flush();
        }
    }

}
