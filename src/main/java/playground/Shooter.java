package playground;

import com.lmax.disruptor.dsl.Disruptor;
import disruptor.RedisEvent;
import io.netty.handler.codec.redis.InlineCommandRedisMessage;
import redis.RedisDisruptorBridge;

public class Shooter extends Thread {
    private Disruptor<RedisEvent> disruptor;

    public Shooter(Disruptor<RedisEvent> disruptor) {
        this.disruptor = disruptor;
    }

    public void run() {
        try {
            final RedisDisruptorBridge redisDisruptorBridge = new RedisDisruptorBridge(disruptor);

            boolean lastFlush = false;
            for (; ; ) {
                if (redisDisruptorBridge.channel.isWritable()) {
                    if (lastFlush) {
                        lastFlush = false;
                    }
                    redisDisruptorBridge.channel.write(new InlineCommandRedisMessage("ping"));
                } else {
                    if (!lastFlush) {
                        redisDisruptorBridge.channel.flush();
                        lastFlush = true;
                    }
                }
            }
        } catch (Exception ex) {
            System.err.println("Something bad happened " + ex);
        }
    }
}
