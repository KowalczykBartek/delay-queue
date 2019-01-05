package contract;

/**
 * Contract for delayed message consumer.
 */
public interface DelayedEventProcessor {

    /**
     * Just an init method, it will be called by {@class loop.CoreLoop} on its startup (once).
     */
    void init();

    /**
     * heart of our application, core-loop will pass down to this method each received message.
     * You can process it (in non-blocking manner) and return boolean value indicated wheter this message
     * should be acked or not.
     *
     * @param message
     * @return indicate acking or not acking (to ack or not to ack ? that is the question !)
     */
    boolean consume(final String message);
}
