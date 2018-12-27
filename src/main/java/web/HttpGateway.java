package web;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.impl.StringEscapeUtils;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scheduling.ScheduleClient;

import java.time.Instant;

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

        router.route(HttpMethod.PUT, "/messages/:messageId")
                .handler(BodyHandler.create())//
                .handler(routingContext -> {
                    final String messageId = routingContext.request().getParam("messageId");
                    final JsonObject bodyAsJson = routingContext.getBodyAsJson();

                    final long when = toTimestamp(bodyAsJson.getString("when"));
                    final String event = bodyAsJson.getJsonObject("event").toString();
                    final HttpServerResponse response = routingContext.response();

                    try {
                        final String safeEvent = StringEscapeUtils.escapeJava(event);


                        scheduleClient.scheduleEvent(when, messageId, safeEvent)
                                .thenAccept(result -> {
                                    /**
                                     * Event is scheduled and processed by thread not owned by Vert.x, because of that,
                                     * this will not allow JVM to optimize synchronization action. TODO, FIXME
                                     */
                                    response.setStatusCode(200).end();
                                })
                                .exceptionally(throwable -> {
                                    LOG.error("Error occurred {}", throwable);
                                    response.setStatusCode(500).end();
                                    return null;
                                });
                    } catch (final Exception exception) {
                        LOG.error("Error occurred {}", exception);
                        response.setStatusCode(500).end();
                    }

                });

        server.requestHandler(router::accept);

        server.listen(8080, "localhost", res -> {
            if (res.succeeded()) {
                LOG.info("Server is now listening!");
            } else {
                LOG.info("Failed to bind!");
            }
        });
    }

    /**
     * Accept both ISO-8601 and plain timestamp.
     *
     * @param date
     * @return
     */
    private long toTimestamp(final String date) {
        try {
            return Long.parseLong(date);
        } catch (final Exception ex) {
            return Instant.parse(date).toEpochMilli();
        }
    }
}
