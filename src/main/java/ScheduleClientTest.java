import scheduling.ScheduleClient;

public class ScheduleClientTest {
    public static void main(String... args) throws InterruptedException {
        final ScheduleClient scheduleClient = new ScheduleClient();
        scheduleClient.awaitConnection();

        scheduleClient.scheduleEvent(System.currentTimeMillis() + 10000, "msg_1", "test");
        scheduleClient.scheduleEvent(System.currentTimeMillis() + 10002, "msg_2", "test");
        scheduleClient.scheduleEvent(System.currentTimeMillis() + 10003, "msg_3", "test");
    }
}