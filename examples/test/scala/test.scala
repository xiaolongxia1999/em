import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import udaf.myAggregator
import org.apache.spark.sql.functions._

/**
  * Created by Administrator on 2018/9/7 0007.
  */

//主构造器和辅助构造器，主构造器可以有多个参数，可以加入val和var声明
//见：https://blog.csdn.net/Captain72/article/details/78855373
class test {
  var a: String = ""
  var b: Int = 0
  def this(a1: String) {
    this()
    this.a = a1
  }

  def this(name: String, age: Int) {
    this(name)

    this.b = age

  }

  def add: Double = b.toDouble + 1
}

object test2 {
  val a =   1.0

  def fwad(a: Double): myAggregator ={
    new myAggregator
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("ff")
    val spark = SparkSession.builder.config(conf).getOrCreate()
//    val path = "C:\\Users\\Administrator\\Desktop\\数慧空间国际化翻译-成亮.csv"
//
//    val rdd = spark.sparkContext.textFile(path)
//    val rdd1 = rdd.map(x => x.split(",").filterNot(y => y.isEmpty).mkString(","))
//
//    rdd1.coalesce(1).saveAsTextFile("C:\\Users\\Administrator\\Desktop\\cl.csv")

    var df = spark.read.option("header", false).csv("C:\\Users\\Administrator\\Desktop\\国际化cl.csv").toDF("f1","f2")
    df.show()

    def upperCaseFirst(str:String):String = {
      val str1 = str.toLowerCase()
      val str2 = (str1.charAt(0).toInt - 32).toChar + str1.drop(1)
      str2
    }
    val upperCaseFirst1:(String=> String) = upperCaseFirst

    val firstUpper = udf(upperCaseFirst1 )

    df = df.withColumn("f3" , firstUpper(col("f2")))
    df.show()

  }
}
