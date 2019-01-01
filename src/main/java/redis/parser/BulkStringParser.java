package redis.parser;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.codec.redis.BulkStringHeaderRedisMessage;
import io.netty.handler.codec.redis.DefaultBulkStringRedisContent;
import io.netty.handler.codec.redis.DefaultLastBulkStringRedisContent;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.concurrent.CompletableFuture;

public class BulkStringParser implements Parser {
    //shared state - always overridden before usage.
    private ByteBuf tmpBuf = PooledByteBufAllocator.DEFAULT.directBuffer();

    final CompletableFuture<Object> completableFuture;

    public BulkStringParser(final CompletableFuture<Object> completableFuture) {
        this.completableFuture = completableFuture;
    }

    @Override
    public boolean parse(final RedisMessage msg) {
        if (msg instanceof BulkStringHeaderRedisMessage) {
            tmpBuf.resetReaderIndex();
            tmpBuf.resetWriterIndex();
            return false;
        } else if (msg instanceof DefaultLastBulkStringRedisContent) {
            final DefaultLastBulkStringRedisContent bulkMsg = (DefaultLastBulkStringRedisContent) msg;
            tmpBuf.writeBytes(bulkMsg.content());
            final byte[] bytes = new byte[tmpBuf.writerIndex()];
            tmpBuf.readBytes(bytes);
            final String message = new String(bytes);
            completableFuture.complete(message);
            return true;
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