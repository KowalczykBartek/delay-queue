import loop.CoreLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import playground.ReceivedMessageBridge;
import web.HttpGateway;

import java.util.function.Consumer;

public class App {
    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main(final String... args) {
        LOG.info("Starting application ...");

        //that is not part of core delay-queue, just to allow to interact from browser.
        final ReceivedMessageBridge receivedMessageBridge = new ReceivedMessageBridge();

        final HttpGateway httpGateway = new HttpGateway();
        httpGateway.start(receivedMessageBridge);

        final Consumer<String> passToWaitingRequest = message -> {
            receivedMessageBridge.receivedMessagesQueue.offer(message);
        };

        final CoreLoop coreLoop = new CoreLoop(passToWaitingRequest);
        coreLoop.run();
    }
}
