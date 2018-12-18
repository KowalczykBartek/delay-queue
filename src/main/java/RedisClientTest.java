import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.RedisClient;

public class RedisClientTest {
    private static final Logger LOG = LoggerFactory.getLogger(RedisClient.class);

    public static void main(String... args) throws InterruptedException {

        final RedisClient redisClient = new RedisClient();
        redisClient.awaitConnection();

        final long currentTimeMillis = System.currentTimeMillis();
        final String messageId = "message_new_" + currentTimeMillis;
        final String body = "{\\\"message\\\" : \\\"test\\\"}";

        final String zaddQuery = "ZADD timeQueue " + currentTimeMillis + " \"" + messageId + "\"";
        redisClient.query(zaddQuery)
                .thenAccept(result -> {
                    LOG.info("{}", result);
                });

        final String hsetQuery = "HSET messages " + messageId + " \"" + body + "\"";
        redisClient.query(hsetQuery)
                .thenAccept(result -> {
                    LOG.info("{}", result);
                })
                .exceptionally(result -> {
                    LOG.error("{}", result);
                    return null;
                });

        final String hgetQuery = "HGET messages " + messageId;
        redisClient.query(hgetQuery)
                .thenAccept(result -> {
                    LOG.info("{}", result);
                })
                .exceptionally(result -> {
                    LOG.error("{}", result);
                    return null;
                });


        redisClient.query("ZRANGE myzset 0 " + System.currentTimeMillis())
                //to not block event loop
                .thenAcceptAsync(object -> {
                    final String[] results = (String[]) object;
                    for (int i = 0; i < results.length; i++) {
                        LOG.info("{}", results[i]);
                    }
                });

        new Thread(() -> {
            while (true) {
                final long milis = System.currentTimeMillis();
                final String query = "ZADD myzset " + milis + " \"message_" + milis + "\"";
                redisClient.query(query)
                        .thenAccept(result -> {
                            LOG.info("{}", result);
                        })
                        .exceptionally(result -> {
                            LOG.error("{}", result);
                            return null;
                        });

                try {
                    Thread.sleep(10);
                } catch (final InterruptedException ex) {
                    LOG.error("Nothing to see here", ex);
                }
            }
        }).start();

        new Thread(() -> {
            while (true) {
                redisClient.query("ZRANGE myzset 0 " + System.currentTimeMillis())//
                        //to not block event loop
                        .thenAcceptAsync(object -> {
                            final String[] results = (String[]) object;
                            for (int i = 0; i < results.length; i++) {
                                LOG.info("{}", results[i]);
                            }
                        });
                try {
                    Thread.sleep(10);
                } catch (final InterruptedException ex) {
                    LOG.error("Nothing to see here", ex);
                }
            }
        }).start();

    }
}
