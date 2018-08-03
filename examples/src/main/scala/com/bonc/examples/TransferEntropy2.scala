package com.bonc.examples



import com.bonc.Interface.IModel
import com.bonc.models.TransferEntropyModel
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by Administrator on 2018/6/22 0022.
  */

object TransferEntropy2 {
  class T
  def main(args: Array[String]): Unit = {

    //单机下
//    val path = "C:\\Users\\Administrator\\Desktop\\abcdefgTemp\\yangsDataNoTranspose.csv"
//    val json_path ="C:\\Users\\Administrator\\Desktop\\传递熵\\spark\\spark运行传递熵\\te_json.txt"
//    val saveModelPath = "F:\\1\\te"
//    val SensorForPredict = 11
//    val master = "local[*]"
//    //    val master = "spark://172.16.32.139:7077"
//    val appName = "TE_Calc"
//    val conf = new SparkConf().setAppName(appName).setMaster(master).set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//
//    @transient
//    val sparkContext = new SparkContext(conf)
//    @transient
//    val spark = SparkSession.builder().master(master).appName(appName).getOrCreate()




//    分布式下
    val path = args(0)
    val json_path =args(1)
    val saveModelPath = args(2)
    val SensorForPredict = args(3).toInt
//    val flag:Boolean = args(4).toBoolean
    val appName = "TE_Calc"
    val conf = new SparkConf().setAppName(appName).set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    @transient
    val sparkContext = new SparkContext(conf)
    @transient
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    val parallelism = 24

    //scala的IO,本地读取
    val jsonStr = Source.fromFile(json_path).mkString
    val df = spark.read.format("csv").option("header",true).load(path)

    println("1st:")
    df.show()

    //预测集(某一列传感器）
    val arrayRowPredict = df.select(df.columns(SensorForPredict)).collect()

//    不使用接口形式
//    val te_model:TransferEntropyModel = new TransferEntropyModel()//TransferEntropy(arrayRow,jsonStr)

    //使用接口形式改变实现类——缺一个类工厂生产，还可用枚举
    val te_model:IModel[Row] = new TransferEntropyModel().asInstanceOf[IModel[Row]]
    val arrayRowPredictT = arrayRowPredict.map(x=>x.asInstanceOf[Row])
    //val arrayRowPredictT = arrayRowPredict.asInstanceOf[Array[Row]]
    //
    //    val inputColIndex =
    println("2nd:"+jsonStr)
    te_model.setParam(jsonStr)

//    println(te_model.threshold)
    //    te_model.train(arrayRow,Array(0,1,2).map(x=>Integer.valueOf(x)),Array(0).map(x=>Integer.valueOf(x)))
    println("load stage!")
    te_model.loadModel(saveModelPath)
    println("predict stage!")
//    不用接口形式
//    te_model.predict(arrayRowPredict)

//    使用接口形式
    te_model.predict(arrayRowPredictT)
    println("done!")
  }
}
