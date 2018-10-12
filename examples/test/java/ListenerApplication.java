import py4j.GatewayServer;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2018/9/25 0025.
 */
public class ListenerApplication {

    List<ExampleListener> listeners =  new ArrayList<ExampleListener>();
    public void registerListener(ExampleListener listener){
        listeners.add(listener);
    }

    public void notifyAllListeners() {
        for (ExampleListener listener: listeners) {
            Object returnValue = listener.notify(this);
            System.out.println(returnValue);
        }
    }

    public String toString(){
        return  "<ListenerApplication> instance";
    }

    public static void main(String[] args) {
        ListenerApplication application = new ListenerApplication();
        GatewayServer server = new GatewayServer(application);
        server.start();
    }
}
