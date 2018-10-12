package mydemo

import java.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql.EsSparkSQL
import org.apache.spark.SparkContext._
import org.elasticsearch.spark._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.elasticsearch.spark.streaming._

import scala.collection.mutable
/**
  * Created by Administrator on 2018/10/10 0010.
  */
object sparkWithEs {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("sparkEs").setMaster("local[*]")
    conf.set("es.index.auto.create","true")
        .set("es.nodes","172.16.32.142")
        .set("port","9200")
    val sc = new SparkContext(conf)
//
//    val numbers  = Map("one"->1, "two"->2,"three"->3)
//    val airports = Map("arrival"->"New York", "end"->"Sidney")
//
//    sc.makeRDD(Seq(numbers,airports)).saveToEs("spark/docs")

//    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

//    应该是继承自RDD
//    val rdd = sc.esRDD()

//    从ES中读取数据
//    val options = Map("pushdown"->"true", "es.nodes"->"172.16.32.142","es.port"->"9200")
//    val df = spark.read.format("org.elasticsearch.spark.sql").options(options).load("data_store/my_274_yppjoucy")
//    df.show()
//    println(df.count())
//    import spark.implicits._
////    df.saveToEs("sid/demo")
//
////    数据保存至ES
//    EsSparkSQL.saveToEs(df, "sid/demo")
//    df.write.format("org.elasticsearch.spark.sql").options(options).save("sid/demo")


//      es-spark-streaming示例
      val ssc = new StreamingContext(sc, Seconds(1))
//      val numbers = Map("one"->4, "two"->5,"three"->6)
      val airports = Map("arrival"->"Peking1", "end"->"Perlin2")

      val rdd = sc.makeRDD(Seq(airports))
      val microbatches = mutable.Queue(rdd)
      ssc.queueStream(microbatches).saveToEs("sid/demoStreaming",Map("es.mapping.id"->"trip"))

      ssc.start()
      ssc.awaitTermination()
  }

}
