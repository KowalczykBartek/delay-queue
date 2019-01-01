package playground;

import org.jctools.queues.SpmcArrayQueue;

/**
 * That is damm simple
 */
public class ReceivedMessageBridge {

    /**
     * No back-pressure - if there is more messages than size  of array, we will drop message.
     * AND YES - this is just public instance variable.
     */
    public SpmcArrayQueue<String> receivedMessagesQueue = new SpmcArrayQueue<>(1000);
}
