package loop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scheduling.ScheduleClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Main component - instance of this class, will query REDIS each X milliseconds for messages to consumer.
 * FIXME ATM message is just ignored.
 */
public class CoreLoop {
    private static final Logger LOG = LoggerFactory.getLogger(CoreLoop.class);

    private final ScheduleClient scheduleClient;

    public CoreLoop() {
        scheduleClient = new ScheduleClient();
    }

    public void run() {
        /*
         * This entire chain is executed in the same thread - entire time - so, under the hood, always
         * the same socket used to communicate with REDIS is used and also, the same thread is used.
         */
        scheduleClient.queryScheduledMessages(getCurrentTimeMillis())
                .thenCompose(queryResult -> {
                    final String[] messages = (String[]) queryResult;

                    if (messages.length == 0) {
                        LOG.info("skipping - no messages to process");
                        final CompletableFuture<Void> empty = new CompletableFuture<>();
                        empty.complete(null);
                        return empty;
                    }

                    LOG.info("Processing {} returned messages", messages.length);

                    final List<CompletableFuture<Object>> results = new ArrayList<>(messages.length / 2);

                    /*
                     * When more than X messaged come at the same moment, loop can be blocked - that is  bad :/
                     * FIXME
                     */
                    for (int i = 0; i < messages.length; i = i + 2) {
                        final String messageId = messages[i];
                        final String score = messages[i + 1];

                        LOG.info("score: {} messageId: {}", score, messageId);

                        final long longScore = Long.parseLong(score);
                        final CompletableFuture<Object> processingResult = scheduleClient.popMessage(longScore, messageId)
                                .thenCompose(popResult -> {
                                    LOG.info("Processing message messageId: {} event: {}", messageId, popResult);
                                    return scheduleClient.ackMessage(messageId);
                                });

                        results.add(processingResult);
                    }

                    return CompletableFuture.allOf(results.toArray(new CompletableFuture[results.size()]));
                })
                .thenAccept(asd -> {
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
