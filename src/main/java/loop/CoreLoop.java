package loop;

import contract.DelayedEventProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scheduling.ScheduleClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Main component - instance of this class, will query REDIS each X milliseconds for messages to consume and if any
 * will be received (so, our delayed-message), core-loop wil pass such message to {@class contract.DelayedEventProcessor} instance.
 */
public class CoreLoop {
    private static final Logger LOG = LoggerFactory.getLogger(CoreLoop.class);

    private final ScheduleClient scheduleClient;
    private final DelayedEventProcessor eventProcessor;

    public CoreLoop(final DelayedEventProcessor eventProcessor) {
        this.scheduleClient = new ScheduleClient();
        this.eventProcessor = eventProcessor;

        eventProcessor.init();
    }

    public void run() {
        /*
         * This entire chain is executed in the same thread - entire time - so, under the hood, always
         * the same socket used to communicate with REDIS is used and also, the same thread is used.
         */
        scheduleClient.queryScheduledMessages(getCurrentTimeMillis())
                .thenCompose(queryResult -> {
                    final String[] messages = queryResult;

                    if (messages.length == 0) {
                        LOG.debug("skipping - no messages to process");
                        final CompletableFuture<Void> empty = new CompletableFuture<>();
                        empty.complete(null);
                        return empty;
                    }

                    LOG.info("Processing {} returned messages", messages.length);

                    final List<CompletableFuture<Object>> results = new ArrayList<>(messages.length / 2);

                    /*
                     * When more than X messaged come at the same moment, loop can be blocked - that is bad :/
                     * FIXME
                     */
                    for (int i = 0; i < messages.length; i = i + 2) {
                        final String messageId = messages[i];
                        final String score = messages[i + 1];

                        LOG.info("score: {} messageId: {}", score, messageId);

                        final long longScore = Long.parseLong(score);
                        final CompletableFuture<Object> processingResult = scheduleClient.popMessage(longScore, messageId)
                                .thenCompose(popResult -> {
                                    LOG.debug("Processing message messageId: {} event: {}", messageId, popResult);
                                    final boolean messageProcessed = eventProcessor.consume((String) popResult);
                                    if (messageProcessed) {
                                        LOG.debug("message with messageId: {} processed - ack");
                                        return scheduleClient.ackMessage(messageId);
                                    } else {
                                        LOG.debug("message with messageId: {} ignored - ack abandoned");
                                        return CompletableFuture.completedFuture(null);
                                    }
                                });

                        results.add(processingResult);
                    }

                    return CompletableFuture.allOf(results.toArray(new CompletableFuture[results.size()]));
                })
                .thenAccept(oneRoundProcessed -> {
                    LOG.debug("Executing CoreLoop.run from netty's executor");
                    scheduleClient.getLoop().schedule(this::run, 500, TimeUnit.MILLISECONDS);
                })
                .exceptionally(throwable -> {
                    LOG.info("Error occurred {}", throwable);
                    scheduleClient.getLoop().schedule(this::run, 500, TimeUnit.MILLISECONDS);
                    return null;
                });
    }

    protected long getCurrentTimeMillis() {
        return System.currentTimeMillis();
    }
}
