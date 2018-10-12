import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

//import scala.reflect.io.File
/**
  * Created by Administrator on 2018/10/2 0002.
  */
class ShuHuiKongJian {

}

object ShuHuiKongJian {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("shkj").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val path = "E:\\1\\chinese.txt"
    val out = "E:\\1\\chinese_out.txt"

//    var df = spark.read.option("header", false).csv(path) //.toDF("field")
//    df = df.distinct().toDF("field").orderBy(col("field").desc)
//    println("df size is" + df.count())
//    df.show()

    val rdd = spark.sparkContext.textFile(path)
    //false为降序
//    val rdd1 = rdd.map(lines => lines.split("@@+").mkString("----->")).coalesce(1).sortBy(x=>x.length,false)
    val rdd1 = rdd.map(lines => lines.replaceAll("[\\s|&nbsp|:|：|;|；|!|！|?|？\\$\\{.+}]+","")

//  .replace(":","")
   .split("@"))
   .flatMap(y=>y).coalesce(1)
    .distinct()
  .sortBy(x=>x.length,false)

    //.flatMap(x=>x).coalesce(1).distinct().sortBy(x=>x.length,false)
//    val rdd1 = rdd.map(lines => lines.split("@")).flatMap(x=>x).coalesce(1).distinct().sortBy(x=>x.length,false)
    println(rdd1.count())

    rdd1.saveAsTextFile(out)

//    df.coalesce(1).write.mode(SaveMode.Overwrite).csv(out)
  }

}
