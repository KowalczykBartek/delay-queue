package redis.parser;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.redis.*;
import org.jctools.queues.MpscChunkedArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.RedisClient;

import java.util.concurrent.CompletableFuture;

public class RedisMessageParser extends SimpleChannelInboundHandler<RedisMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisClient.class);

    private final MpscChunkedArrayQueue<CompletableFuture<Object>> requestsQueue;

    private Parser parser;
    private CompletableFuture<Object> currentContextFuture;

    public RedisMessageParser(final int maxConcurrentRequests) {
        requestsQueue = new MpscChunkedArrayQueue<>(maxConcurrentRequests);
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final RedisMessage msg) {

        if (currentContextFuture == null) {
            currentContextFuture = requestsQueue.poll();
        }

        /*
         * Error handling is common for all parsers - just complete exceptionally.
         */
        if (msg instanceof ErrorRedisMessage) {
            final ErrorRedisMessage errorRedisMessage = (ErrorRedisMessage) msg;
            currentContextFuture.completeExceptionally(new RuntimeException(errorRedisMessage.content()));
            clearCurrentContext();
            return;
        }

        if ((msg instanceof ArrayHeaderRedisMessage) && parser == null) {
            parser = new RedisArrayParser(currentContextFuture);
            parse(msg);
        } else if ((msg instanceof BulkStringHeaderRedisMessage) && parser == null) {
            parser = new BulkStringParser(currentContextFuture);
            parse(msg);
        } else if ((msg instanceof IntegerRedisMessage) && parser == null) {
            parser = new IntegerResponseParser(currentContextFuture);
            parse(msg);
        } else if (parser != null) {
            parse(msg);
        } else {
            currentContextFuture.completeExceptionally(new RuntimeException("Panic ! not able to parse message"));
            clearCurrentContext();
        }
    }

    private void clearCurrentContext() {
        parser = null;
        currentContextFuture = null;
    }

    private void parse(final RedisMessage msg) {
        if (parser.parse(msg)) {
            clearCurrentContext();
        }
    }

    public boolean registerCallback(final CompletableFuture<Object> completableFuture) {
        return requestsQueue.offer(completableFuture);
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
        currentContextFuture = null;
        parser = null;
        LOG.error("Error occurred {}", cause);
    }
}
