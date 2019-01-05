package example;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.jctools.queues.SpmcArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Handler created to enable integration from the web (for example curl).
 * Each incoming connection is register inside Set, and all subsequent received messages (from fetching loop)
 * are pushed to awaiting client, using simplest HTTP chunking :)
 */
public class ExampleNotificationHandler implements Handler<RoutingContext> {

    private static final Logger LOG = LoggerFactory.getLogger(ExampleNotificationHandler.class);
    private final SpmcArrayQueue<String> receivedMessagesQueue;

    //that is not thread-safe implementation because all accessed are from the same thread.
    private final Set<HttpServerResponse> waitingRequests = new HashSet<>();

    public ExampleNotificationHandler(final Vertx vertx, final SpmcArrayQueue<String> receivedMessagesQueue) {
        this.receivedMessagesQueue = receivedMessagesQueue;

        /*
         * Because implementing PUSH mechanism on all connections would be more complex than just periodic fetch
         * already received messages - I did pull - this is sufficient, especially that this is just example.
         */
        vertx.setPeriodic(100, hmm -> {

            final Iterator<HttpServerResponse> iterator = waitingRequests.iterator();
            /*
             * That is quite relaxed way to get messages, but even if we drop something, nothing special happens.
             */
            final List<String> messages = new ArrayList<>();
            receivedMessagesQueue.drain(messages::add);

            while (iterator.hasNext()) {
                final HttpServerResponse request = iterator.next();
                messages.forEach(message -> {
                    try {
                        request.write(message);
                    } catch (final Exception ex) {
                        /*
                         * In case of any problem with connection, just remove connection from waiting list
                         * and try to close connection (just to be sure that is closed).
                         */
                        waitingRequests.remove(request);
                        try {
                            request.close();
                        } catch (final Exception closingException) {
                            LOG.error("Not able to close broken connection", closingException);
                        }
                    }
                });
            }
        });
    }

    @Override
    public void handle(final RoutingContext event) {
        final HttpServerRequest request = event.request();
        event.request().response().setChunked(true);
        waitingRequests.add(request.response());
    }
}
