import org.xml.sax.InputSource;
import scala.xml.Source;

/**
 * Created by Administrator on 2018/9/13 0013.
 */
public class LogDemo {
    public static void main(String[] args) {
        Integer a = 1;
        String b = "sa";
        int af = 2;


        InputSource inputSource = null;
        try {
            inputSource = Source.fromFile("file:/C:/Users/魏永朝/Desktop/能源管理平台3期/异常侦测应用场景/data/ProcessedAbnormalDetection.csv");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("some error!-------------------------------");
        }
        System.out.println(inputSource);
    }
}
