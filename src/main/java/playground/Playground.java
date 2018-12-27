package playground;

import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import disruptor.RedisEvent;


public class Playground {
    public static void main(String... args) {
        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = 1024;

        // Construct the Disruptor
        Disruptor<RedisEvent> disruptor = new Disruptor<>(RedisEvent::new, bufferSize, DaemonThreadFactory.INSTANCE);

        // Connect the handler
        disruptor.handleEventsWith((event, sequence, endOfBatch) -> doNothing());

        // Start the Disruptor, starts all threads running
        disruptor.start();

        //start threads requesting REDIS.
        new Shooter(disruptor).run();
        new Shooter(disruptor).run();
        new Shooter(disruptor).run();
        new Shooter(disruptor).run();
        new Shooter(disruptor).run();
    }

    public static void doNothing() {
    }
}
