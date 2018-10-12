import py4j.GatewayServer;

/**
 * Created by Administrator on 2018/9/25 0025.
 */
public class ExampleClientApplication {
    public static void main(String[] args) {
        GatewayServer server = new GatewayServer();
        server.start();
        IHello hello = (IHello) server.getPythonServerEntryPoint(new Class[]{IHello.class});
        hello.sayHello();
        hello.sayHello(3, "Hello a");
        server.shutdown();

    }
}
