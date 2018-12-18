package redis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.redis.*;
import org.jctools.queues.MpscChunkedArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class Dispatcher extends SimpleChannelInboundHandler<RedisMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisClient.class);

    private final MpscChunkedArrayQueue<CompletableFuture<Object>> requestsQueue;

    private FunnyParser parser;
    private CompletableFuture<Object> currentContextFuture;

    public Dispatcher(final int maxConcurrentRequests) {
        requestsQueue = new MpscChunkedArrayQueue<>(maxConcurrentRequests);
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final RedisMessage msg) {

        if (currentContextFuture == null) {
            currentContextFuture = requestsQueue.poll();
        }

        if ((msg instanceof ArrayHeaderRedisMessage) && parser == null) {
            parser = new RedisArrayParser();
            parse(msg);
        } else if ((msg instanceof BulkStringHeaderRedisMessage) && parser == null) {
            parser = new BulkStringParser();
            parse(msg);
        } else if ((msg instanceof IntegerRedisMessage) && parser == null) {
            parser = new IntegerResponseParser();
            parse(msg);
        } else if (parser != null) {
            parse(msg);
        } else {
            currentContextFuture.completeExceptionally(new RuntimeException("Panic ! not able to parse message"));
            parser = null;
            currentContextFuture = null;
        }
    }

    private void parse(final RedisMessage msg) {
        if (parser.parse(msg, currentContextFuture)) {
            parser = null;
            currentContextFuture = null;
        }
    }

    public interface FunnyParser {
        boolean parse(final RedisMessage msg, final CompletableFuture<Object> completableFuture);
    }

    public static class IntegerResponseParser implements FunnyParser {

        @Override
        public boolean parse(RedisMessage msg, final CompletableFuture<Object> completableFuture) {

            if (msg instanceof ErrorRedisMessage) {
                final ErrorRedisMessage errorRedisMessage = (ErrorRedisMessage) msg;
                completableFuture.completeExceptionally(new RuntimeException(errorRedisMessage.content()));
                return true;
            }

            if (msg instanceof IntegerRedisMessage) {
                //just an element and last element.
                final IntegerRedisMessage integerMsg = (IntegerRedisMessage) msg;
                completableFuture.complete(integerMsg.value());
                return true;
            } else {
                completableFuture.completeExceptionally(new RuntimeException("Panic ! not able to parse message"));
                return true;
            }
        }
    }

    public static class BulkStringParser implements FunnyParser {
        //shared state - always overridden before usage.
        private ByteBuf tmpBuf = Unpooled.buffer();
        private long length = 0;

        @Override
        public boolean parse(final RedisMessage msg, final CompletableFuture<Object> completableFuture) {

            if (msg instanceof ErrorRedisMessage) {
                final ErrorRedisMessage errorRedisMessage = (ErrorRedisMessage) msg;
                completableFuture.completeExceptionally(new RuntimeException(errorRedisMessage.content()));
                return true;
            }

            if (msg instanceof BulkStringHeaderRedisMessage) {
                final BulkStringHeaderRedisMessage bulkMessage = (BulkStringHeaderRedisMessage) msg;
                length = bulkMessage.bulkStringLength();
                tmpBuf.capacity((int) length);
                tmpBuf.resetReaderIndex();
                tmpBuf.resetWriterIndex();
                return false;
            } else if (msg instanceof DefaultLastBulkStringRedisContent) {
                final DefaultLastBulkStringRedisContent bulkMsg = (DefaultLastBulkStringRedisContent) msg;
                bulkMsg.content().readBytes(tmpBuf);
                final byte[] bytes = new byte[tmpBuf.capacity()];
                tmpBuf.readBytes(bytes);
                final String message = new String(bytes);

                try {
                    completableFuture.complete(message);
                } catch (final Exception ex) {
                    System.gc();
                }

                return true;
            } else if (msg instanceof DefaultBulkStringRedisContent) {
                final DefaultBulkStringRedisContent bulkMsg = (DefaultBulkStringRedisContent) msg;
                bulkMsg.content().readBytes(tmpBuf);
                return false;
            } else {
                completableFuture.completeExceptionally(new RuntimeException("Panic ! not able to parse message"));
                return true;
            }
        }
    }

    public static class RedisArrayParser implements FunnyParser {

        //shared state - always overridden before usage.
        private ByteBuf tmpBuf = Unpooled.buffer();
        private long length = 0;
        private long index = 0;
        private String[] messages;
        private long readLength;

        @Override
        public boolean parse(RedisMessage msg, final CompletableFuture<Object> completableFuture) {

            if (msg instanceof ErrorRedisMessage) {
                final ErrorRedisMessage errorRedisMessage = (ErrorRedisMessage) msg;
                completableFuture.completeExceptionally(new RuntimeException(errorRedisMessage.content()));
                return true;
            }

            if (msg instanceof ArrayHeaderRedisMessage) {
                length = ((ArrayHeaderRedisMessage) msg).length();
                messages = new String[(int) length];
                return false;
            } else if (msg instanceof BulkStringHeaderRedisMessage) {
                final BulkStringHeaderRedisMessage bulkMsg = (BulkStringHeaderRedisMessage) msg;
                readLength = bulkMsg.bulkStringLength();
                tmpBuf.capacity((int) readLength);
                tmpBuf.resetReaderIndex();
                tmpBuf.resetWriterIndex();
                return false;
            } else if (msg instanceof DefaultLastBulkStringRedisContent) {
                final DefaultLastBulkStringRedisContent bulkMsg = (DefaultLastBulkStringRedisContent) msg;
                tmpBuf.writeBytes(bulkMsg.content());
                final byte[] bytes = new byte[tmpBuf.capacity()];
                tmpBuf.readBytes(bytes);
                final String message = new String(bytes);

                //TODO XD
                messages[(int) index++] = message;
                if (index >= length) {
                    completableFuture.complete(messages);
                    return true;
                }

                return false;
            } else if (msg instanceof DefaultBulkStringRedisContent) {
                final DefaultBulkStringRedisContent bulkMsg = (DefaultBulkStringRedisContent) msg;
                tmpBuf.writeBytes(bulkMsg.content());
                return false;
            } else {
                completableFuture.completeExceptionally(new RuntimeException("Panic ! not able to parse message"));
                return true;
            }
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
