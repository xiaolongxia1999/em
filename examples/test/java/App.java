import py4j.GatewayServer;

/**
 * Created by Administrator on 2018/9/25 0025.
 */
public class App {
    public int addtion(int first, int second) {
        return first + second;
    }

    public static void main(String[] args) {
        App app = new App();
        GatewayServer server = new GatewayServer();
        server.start();
    }
}
