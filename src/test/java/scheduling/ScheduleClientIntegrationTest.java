package scheduling;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;
import redis.embedded.RedisServer;

import java.io.IOException;
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
    public void clear() {
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

    @Test
    public void shouldReturnMessageToPop() {
        //given
        final long tenMinutes = 10 * 1000;
        final long moreThanTenMinutes = 11 * 1000;
        final long toBeScheduled = System.currentTimeMillis() + tenMinutes;
        final String messageId = "messageId";
        final String sampleMessage = "test";

        //when
        scheduleClient.scheduleEvent(toBeScheduled, messageId, sampleMessage).join();
        final String[] emptyResult = (String[]) scheduleClient.queryMessages(System.currentTimeMillis()).join();
        final String[] scheduledEvent = (String[]) scheduleClient.queryMessages(System.currentTimeMillis() + moreThanTenMinutes).join();

        //then
        assertThat(emptyResult).isEmpty();
        assertThat(scheduledEvent).hasSize(1);
        assertThat(scheduledEvent[0]).isEqualTo(messageId);
    }

    @Test
    public void shouldHandlerMoreThanOneScheduledMessage() {
        //given
        final long tenMinutes = 10 * 1000;
        final long moreThanTenMinutes = 11 * 1000;
        final long toBeScheduled = System.currentTimeMillis() + tenMinutes;
        final String messageId = "messageId";
        final String sampleMessage = "test";

        //when
        scheduleClient.scheduleEvent(toBeScheduled, messageId + "1", sampleMessage).join();
        scheduleClient.scheduleEvent(toBeScheduled, messageId + "2", sampleMessage).join();
        scheduleClient.scheduleEvent(toBeScheduled, messageId + "3", sampleMessage).join();
        scheduleClient.scheduleEvent(toBeScheduled, messageId + "4", sampleMessage).join();

        final String[] scheduledEvent = (String[]) scheduleClient.queryMessages(System.currentTimeMillis() + moreThanTenMinutes).join();

        //then
        assertThat(scheduledEvent).hasSize(4);
        assertThat(scheduledEvent[0]).isEqualTo(messageId + "1");
        assertThat(scheduledEvent[1]).isEqualTo(messageId + "2");
        assertThat(scheduledEvent[2]).isEqualTo(messageId + "3");
        assertThat(scheduledEvent[3]).isEqualTo(messageId + "4");
    }

    @Test
    public void shouldPopMessage() {
        //given
        final long tenMinutes = 10 * 1000;
        final long toBeScheduled = System.currentTimeMillis() + tenMinutes;
        final String messageId = "messageId";
        final String sampleMessage = "test";

        //when
        scheduleClient.scheduleEvent(toBeScheduled, messageId, sampleMessage).join();

        //get the message
        final String message = (String) scheduleClient.popMessage(messageId).join();

        //then
        assertThat(message).isEqualTo(sampleMessage);

        //message should also be put to UNACK queue
        final Set<String> unacks = jedis.zrange("unacks", 0, -1);
        assertThat(unacks).hasSize(1);
        assertThat(unacks.iterator().next()).isEqualTo(messageId);

        //delayed message should be removed from core queue
        final Set<String> queue = jedis.zrange("queue", 0, -1);
        assertThat(queue).isEmpty();

    }
}
