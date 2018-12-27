import loop.CoreLoop;
import web.HttpGateway;

public class App {
    public static void main(final String... args) {
        final HttpGateway httpGateway = new HttpGateway();
        httpGateway.start();

        final CoreLoop coreLoop = new CoreLoop();

        coreLoop.run();
    }

}
