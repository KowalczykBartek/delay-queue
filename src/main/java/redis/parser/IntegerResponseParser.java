package redis.parser;

import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.concurrent.CompletableFuture;

public class IntegerResponseParser implements Parser {

    private final CompletableFuture<Object> completableFuture;

    public IntegerResponseParser(final CompletableFuture<Object> completableFuture) {
        this.completableFuture = completableFuture;
    }

    @Override
    public boolean parse(RedisMessage msg) {
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