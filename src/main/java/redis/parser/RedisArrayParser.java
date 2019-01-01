package redis.parser;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.codec.redis.*;

import java.util.concurrent.CompletableFuture;

public class RedisArrayParser implements Parser {
    private static final String[] EMPTY_ARRAY = new String[]{};
    //shared state - always overridden before usage.
    private ByteBuf tmpBuf = PooledByteBufAllocator.DEFAULT.directBuffer();
    private int length = 0;
    private int currentMessageIndex = 0;
    private String[] responseElementsArray;

    private final CompletableFuture<Object> completableFuture;

    public RedisArrayParser(final CompletableFuture<Object> completableFuture) {
        this.completableFuture = completableFuture;
    }

    @Override
    public boolean parse(final RedisMessage msg) {
        if (msg instanceof ArrayHeaderRedisMessage) {
            //for now, because no stream support, we will return exception when there is more elements than INT_MAX
            if (((ArrayHeaderRedisMessage) msg).length() > Integer.MAX_VALUE) {
                completableFuture.completeExceptionally(new RuntimeException("More than " + Integer.MAX_VALUE + " elements received. Not able to parse."));
                return true;
            }
            length = (int) ((ArrayHeaderRedisMessage) msg).length();
            responseElementsArray = new String[length];
            if (length == 0) {
                completableFuture.complete(EMPTY_ARRAY);
                return true;
            }
            return false;
        } else if (msg instanceof BulkStringHeaderRedisMessage) {
            tmpBuf.resetReaderIndex();
            tmpBuf.resetWriterIndex();
            return false;
        } else if (msg instanceof DefaultLastBulkStringRedisContent) {
            final DefaultLastBulkStringRedisContent bulkMsg = (DefaultLastBulkStringRedisContent) msg;

            tmpBuf.writeBytes(bulkMsg.content());
            final byte[] bytes = new byte[tmpBuf.writerIndex()];
            tmpBuf.readBytes(bytes);
            final String message = new String(bytes);

            responseElementsArray[currentMessageIndex] = message;
            if (currentMessageIndex == (length - 1)) {
                completableFuture.complete(responseElementsArray);
                return true;
            }
            currentMessageIndex++;
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