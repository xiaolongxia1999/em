import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by Administrator on 2018/9/26 0026.
 */
public class Test {

    public static void main(String[] args) throws IOException, InterruptedException {
        String s = System.getenv("PYTHON_HOME");
        System.out.println(s);
        String cmd = s + "\\python.exe -c \"print(2+3) \"";
//        System.out.println(cmd);
//        System.out.println(s +"\\python.exe E:\\IdeaWorkSpace\\EnergyManagement4\\EnergyManagement\\python\\python\\MOPSO\\src\\mopso\\main.py");
        Process process = Runtime.getRuntime().exec(s +"\\python.exe E:\\IdeaWorkSpace\\EnergyManagement4\\EnergyManagement\\python\\python\\MOPSO\\src\\mopso\\main.py");
//        System.out.println("done");
//        Process process = Runtime.getRuntime().exec(cmd);

        //获取输入流和错误流————如果只获取正确的输入流，那错误它是不打印给你的
        BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader err = new BufferedReader(new InputStreamReader(process.getErrorStream()));
        String line;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }

        while ((line = err.readLine()) != null) {
            System.out.println(line);
        }

        in.close();
        err.close();
        process.waitFor();
    }
}
