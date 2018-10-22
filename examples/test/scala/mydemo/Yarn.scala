package mydemo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/10/16 0016.
  */
object Yarn {
    case class stu(name:String, age:Int)
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("spark://172.16.32.139:7077").setAppName("yarn-test")
//                .set("yarn.resourcemanager.address","172.16.3.44:23140")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        import spark.implicits._
        val df = Seq(stu("sid",11), stu("zn", 12)).toDS().toDF()
        df.show()
    }
}
