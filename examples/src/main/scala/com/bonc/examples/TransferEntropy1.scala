package com.bonc.examples

import com.bonc.Interface.IModel
import com.bonc.models.TransferEntropyModel
import net.sf.json.JSONObject
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

//import shapeless._

import scala.io.Source
//import TransferEntropy.spark.models.TransferEntropy

/**
  * Created by Administrator on 2018/6/20 0020.
  */



object TransferEntropy1{
//  class T
//  classOf[T].cast(Row)
  def main(args: Array[String]): Unit = {
////单机下
//    val path = "C:\\Users\\Administrator\\Desktop\\abcdefgTemp\\yangsDataNoTranspose.csv"
//    val json_path ="C:\\Users\\Administrator\\Desktop\\传递熵\\spark\\spark运行传递熵\\te_json.txt"
//    val saveModelPath = "F:\\1\\te"
////val saveModelPath = "/usr/cl"
//    val master = "local[*]"
////    val master = "spark://172.16.32.139:7077"
//    val appName = "TE_Calc"
//    val conf = new SparkConf().setAppName(appName).setMaster(master).set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//
//    @transient
//    val sparkContext = new SparkContext(conf)
//    @transient
//    val spark = SparkSession.builder().master(master).appName(appName).getOrCreate()

//    //分布式下
//    val path = args(0)
//    val json_path =args(1)
//    val saveModelPath = args(2)
//    val appName = "TE_Calc"
//    val conf = new SparkConf().setAppName(appName).set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//    @transient
//    val sparkContext = new SparkContext(conf)
//    @transient
//    val spark = SparkSession.builder().appName(appName).getOrCreate()
//    val parallelism = 24
  val path = args(0)
  val json_path =args(1)
  val saveModelPath = args(2)
  val master = "local[*]"
  ////    val master = "spark://172.16.32.139:7077"
  val appName = "TE_Calc"
  val conf = new SparkConf().setAppName(appName).setMaster(master).set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
  //
  //    @transient
  //    val sparkContext = new SparkContext(conf)
  //    @transient
  val spark = SparkSession.builder().master(master).appName(appName).getOrCreate()

    //scala的IO,本地读取
    val jsonStr = Source.fromFile(json_path).mkString//.mkString
    val df = spark.read.format("csv").option("header",true).load(path)

    println("1st:")
    df.show()
    //训练集
    val arrayRowTrain = df.collect()
    //预测集
    //val arrayRowPredict = df.select(df.columns(2)).collect()

//    val te_model:TransferEntropyModel = new TransferEntropyModel()//TransferEntropy(arrayRow,jsonStr)



//    abstract class T extends Row
    //使用接口形式改变实现类——缺一个类工厂生产，还可用枚举
    //给该泛型接口提供类型：Row
      val te_model:IModel[Row] = new TransferEntropyModel().asInstanceOf[IModel[Row]]
      val arrayRowTrainT = arrayRowTrain.map(x=>x.asInstanceOf[Row])

  //这种不行
//    val te_model:IModel[AnyRef] = new TransferEntropyModel().asInstanceOf[IModel[AnyRef]]
//    val arrayRowTrainT = arrayRowTrain.map(x=>x.asInstanceOf[AnyRef])



    //4个隐藏参数-需要在模型外部解析出来：
    val paramJson = JSONObject.fromObject(jsonStr)
    val inputColIndex = paramJson.getJSONArray("inputColIndex").toArray.map(x=>Integer.valueOf(x.toString.toInt))
    val outputColIndex = paramJson.getJSONArray("outputColIndex").toArray.map(x=>Integer.valueOf(x.toString.toInt))


    println("2nd:"+jsonStr)

    //直接定义死te_model具体类时
//    te_model.train(arrayRowTrain,te_model.inputColIndex.map(x=>Integer.valueOf(x)),te_model.outputColIndex.map(x=>Integer.valueOf(x)))
    //使用IModel向下转型时——这样模型内部需要从Array[T]重新转型回Array[Row]

    //注意：用Array[T]的好处————因为收集的结果，不一定是Array[Row] ,有可能是Double,Double[][]
    //所以不能直接写死为Array[Row] ,而是允许其他形式的结果，并转型为为Array[T]
    te_model.setParam(jsonStr)

    te_model.train(arrayRowTrainT,inputColIndex.map(x=>Integer.valueOf(x)),outputColIndex.map(x=>Integer.valueOf(x)))

    println("train stage!")
    te_model.saveModel(saveModelPath)



//    println("load stage!")
//    te_model.loadModel(saveModelPath)
//    println("predict stage!")
//    te_model.predict(arrayRowPredict)
//    println("done!")



  }
}
