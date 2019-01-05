package example;

import contract.DelayedEventProcessor;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.StaticHandler;
import org.jctools.queues.SpmcArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import web.HttpGateway;

/**
 * Example delayed message consumer. Just start HTTP server on port 9090 and expose single GET /subscribe endpoint.
 */
public class ExampleConsumer implements DelayedEventProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(HttpGateway.class);

    private SpmcArrayQueue<String> receivedMessagesQueue = new SpmcArrayQueue<>(1000);
    private final Vertx vertx;

    public ExampleConsumer() {
        vertx = Vertx.vertx();
    }

    @Override
    public void init() {
        final HttpServer server = vertx.createHttpServer();
        final Router router = Router.router(vertx);

        router.route("/*").handler(StaticHandler.create());

        /**
         * This is not part of the API.
         */
        final ExampleNotificationHandler exampleNotificationHandler = new ExampleNotificationHandler(vertx, receivedMessagesQueue);
        router.route(HttpMethod.GET, "/subscribe")
                .handler(exampleNotificationHandler);

        server.requestHandler(router::accept);

        server.listen(9090, "0.0.0.0", res -> {
            if (res.succeeded()) {
                LOG.info("Subscription Server is now listening !");
            } else {
                LOG.info("Failed to bind subscription server !");
            }
        });
    }

    @Override
    public boolean consume(final String message) {
        receivedMessagesQueue.offer(message);
        return true;
    }

}
