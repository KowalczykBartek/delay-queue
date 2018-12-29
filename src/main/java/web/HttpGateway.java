package web;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scheduling.ScheduleClient;

/**
 * Expose HTTP api to schedule an event.
 */
public class HttpGateway {
    private static final Logger LOG = LoggerFactory.getLogger(HttpGateway.class);

    public void start() {
        final Vertx vertx = Vertx.vertx();

        final ScheduleClient scheduleClient = new ScheduleClient();
        final HttpServer server = vertx.createHttpServer();
        final Router router = Router.router(vertx);

        final StoreMessageHandler storeMessageHandler = new StoreMessageHandler(scheduleClient);

        router.route("/*").handler(StaticHandler.create());

        router.route(HttpMethod.PUT, "/messages/:messageId")
                .handler(BodyHandler.create())//
                .handler(storeMessageHandler);

        router.route(HttpMethod.POST, "/messages")
                .handler(BodyHandler.create())//
                .handler(storeMessageHandler);


        server.requestHandler(router::accept);

        server.listen(8080, "0.0.0.0", res -> {
            if (res.succeeded()) {
                LOG.info("Server is now listening!");
            } else {
                LOG.info("Failed to bind!");
            }
        });
    }

}
