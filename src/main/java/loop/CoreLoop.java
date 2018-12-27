package loop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scheduling.ScheduleClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class CoreLoop {
    private static final Logger LOG = LoggerFactory.getLogger(CoreLoop.class);

    private final ScheduleClient scheduleClient;

    public CoreLoop() {
        scheduleClient = new ScheduleClient();
    }

    public void run() {
        scheduleClient.queryScheduledMessages(getCurrentTimeMillis())
                .thenCompose(queryResult -> {
                    final String[] messages = (String[]) queryResult;

                    LOG.info("Processing returned messages");

                    final List<CompletableFuture<Object>> results = new ArrayList<>(messages.length / 2);

                    for (int i = 0; i < messages.length; i = i + 2) {
                        final String messageId = messages[i];
                        final String score = messages[i + 1];

                        LOG.info("score: {} messageId: {}", score, messageId);

                        final long longScore = Long.parseLong(score);
                        final CompletableFuture<Object> processingResult = scheduleClient.popMessage(longScore, messageId)
                                .thenCompose(popResult -> {

                                    LOG.info("Processing message messageId: {} event: {}", messageId, popResult);
                                    //MESSAGE PROCESSING HERE
                                    return scheduleClient.ackMessage(messageId);
                                });

                        results.add(processingResult);
                    }

                    return CompletableFuture.allOf(results.toArray(new CompletableFuture[results.size()]));
                })
                .thenAccept(asd -> {
                    LOG.info("Executing CoreLoop.run from netty's executor");
                    scheduleClient.getLoop().schedule(this::run, 1, TimeUnit.SECONDS);
                })
                .exceptionally(throwable -> {
                    LOG.info("Error occurred {}", throwable);
                    scheduleClient.getLoop().schedule(this::run, 1, TimeUnit.SECONDS);
                    return null;
                });
    }

    protected long getCurrentTimeMillis() {
        return System.currentTimeMillis();
    }
}
