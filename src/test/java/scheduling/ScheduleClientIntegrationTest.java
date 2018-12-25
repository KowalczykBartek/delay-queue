package scheduling;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Test run on real embedded redis.
 */
public class ScheduleClientIntegrationTest {

    private static RedisServer redisServer;
    private static ScheduleClient scheduleClient;
    private static Jedis jedis;

    @BeforeClass
    public static void setUp() throws IOException, InterruptedException {
        redisServer = new RedisServer(6379);
        redisServer.start();

        scheduleClient = new ScheduleClient();
        scheduleClient.awaitConnection();

        /**
         * Used for verification.
         */
        jedis = new Jedis("localhost");
    }

    @AfterClass
    public static void destroy() {
        redisServer.stop();
    }

    @After
    public void clear()
    {
        jedis.del("queue");
        jedis.del("messages");
    }

    @Test
    public void shouldScheduleEvent() {
        //given
        final long tenMinutes = 10 * 1000;
        final long toBeScheduled = System.currentTimeMillis() + tenMinutes;
        final String messageId = "messageId";
        final String sampleMessage = "test";
        //when

        scheduleClient.scheduleEvent(toBeScheduled, messageId, sampleMessage).join();

        //then
        final String messages = jedis.hget("messages", messageId);
        assertThat(messages).isEqualTo(sampleMessage);

        final Set<Tuple> queueResult = jedis.zrangeByScoreWithScores("queue", 0, toBeScheduled);

        assertThat(queueResult.size()).isEqualTo(1);
        final Tuple next = queueResult.iterator().next();
        assertThat(next.getScore()).isEqualTo(toBeScheduled);
        assertThat(next.getElement()).isEqualTo(messageId);
    }
}
