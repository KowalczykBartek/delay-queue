import web.HttpGateway;

public class App {
    public static void main(final String... args)
    {
        HttpGateway httpGateway = new HttpGateway();
        httpGateway.start();
    }

}
