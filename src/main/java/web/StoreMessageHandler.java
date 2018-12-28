package web;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.impl.StringEscapeUtils;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scheduling.ScheduleClient;

import java.time.Instant;
import java.util.UUID;

/**
 * Handle request to store delayed message in Redis.
 */
public class StoreMessageHandler implements Handler<RoutingContext> {

    private static final Logger LOG = LoggerFactory.getLogger(StoreMessageHandler.class);

    private final ScheduleClient scheduleClient;

    public StoreMessageHandler(final ScheduleClient scheduleClient) {
        this.scheduleClient = scheduleClient;
    }

    @Override
    public void handle(RoutingContext routingContext) {

        String messageId = routingContext.request().getParam("messageId");
        if (messageId == null) {
            //if there is no messageId, we will generate it.
            messageId = UUID.randomUUID().toString();
        }

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
    }

    /**
     * Accept both ISO-8601 and plain timestamp.
     *
     * @param date
     * @return timestamp represented as long parsed from String (date or timestamp)
     */
    private long toTimestamp(final String date) {
        try {
            return Long.parseLong(date);
        } catch (final Exception ex) {
            return Instant.parse(date).toEpochMilli();
        }
    }
}
