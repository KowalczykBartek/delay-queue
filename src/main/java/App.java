import example.ExampleConsumer;
import loop.CoreLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import web.HttpGateway;

/**
 * Starter !
 */
public class App {
    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main(final String... args) {
        LOG.info("Starting application ...");

        final HttpGateway httpGateway = new HttpGateway();
        httpGateway.start();

        final CoreLoop coreLoop = new CoreLoop(new ExampleConsumer());
        coreLoop.run();
    }
}
