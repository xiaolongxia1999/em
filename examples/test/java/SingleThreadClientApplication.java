import py4j.ClientServer;

/**
 * Created by Administrator on 2018/9/25 0025.
 */
public class SingleThreadClientApplication {
    public static void main(String[] args) {
        ClientServer cs = new ClientServer(null);
        IHello hello = (IHello) cs.getPythonServerEntryPoint(new Class[]{IHello.class});
        System.out.println(hello.sayHello());
        System.out.println(hello.sayHello(5,"Hello World"));
        cs.shutdown();
    }
}
