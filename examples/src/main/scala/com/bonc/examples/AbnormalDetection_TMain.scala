package com.bonc.examples

import com.bonc.Interface.IModel
import com.bonc.models.AbnormalDetection
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, sql}


/**
  * Created by patrick on 2018/4/13.
  * 作者：
  */
object AbnormalDetection_TMain{

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
  var train_result = -1
  var savemodel_result = -1
  var loadmodel_result = -1
  val conf = new SparkConf().set("spark.driver.memory","2g").set("spark.executor.memory","2g").setAppName("KAD").setMaster("local[*]")
  //    val conf = new SparkConf().setAppName("AbnormalDetection").setMaster("spark://172.16.32.139:7077")  //biop 集群
  //    val conf = new SparkConf().set("spark.driver.memory","2g").set("spark.executor.memory","2g").setAppName("KAD").setMaster("local")
  val spark = new sql.SparkSession.Builder().config(conf).getOrCreate()
  def main(args: Array[String]): Unit = {


    val inputfile = args(0)   //参数1：输入数据路径
    val jsonfile = args(1)
    val modelFilePath = args(2)
    var is_Training =  args(3)  //是否为训练阶段
    var is_evaluation =  args(4)
    var is_predict =  args(5)



//    val inputfile = "C:\\Users\\魏永朝\\Desktop\\能源管理平台3期\\异常侦测应用场景\\data\\ProcessedAbnormalDetection.csv"
  //  val inputfile = "/usr/wyz/Abnormal/ProcessedAbnormalDetection.csv"
//    val modelFilePath = "C:\\Users\\魏永朝\\Desktop\\能源管理平台3期\\异常侦测应用场景\\savepath\\AbnormalDetectionModel"
//    val modelFilePath = "/usr/wyz/Abnormal/savepath"
    import spark.implicits._
//    val abnormal = new AbnormalDetection
    val abnormal:IModel[Row] = new AbnormalDetection().asInstanceOf[IModel[Row]]
    val df = spark.read.option("header",true).option("inferSchema",true).csv(inputfile)


    df.cache()
//    var is_Training = 0  //是否为训练阶段
//    var is_evaluation = 0
//    var is_predict = 1

    val ms = 0.95   //训练和测试集划分时刻比例

    val timeCon = (df.sort("time").take((df.count()*ms).toInt).last).get(0)


    //根据时间节点划分测试集和训练集
    val trainData = df.filter($"time"<=timeCon)
    val testData = df.filter($"time">timeCon)

    trainData.cache()
    testData.cache()

   //
//    val jsonfile = "C:\\Users\\魏永朝\\Desktop\\能源管理平台3期\\异常侦测应用场景\\参数json\\json_fpgrowth_train_1_test3.txt"
//    val jsonfile = "/usr/wyz/Abnormal/json_fpgrowth_train.txt"
    val param = spark.read.json(jsonfile).toJSON.select("value").collect().head(0).toString
    abnormal.setParam(param)
    //若非训练阶段则跳过训练和保存模型

    if(is_Training.toInt == 1){
      println("=====================train=====================")
      var starttime=System.nanoTime
      train_result= abnormal.train(trainData.collect(),Array(1),Array(0))
      var endtime=System.nanoTime     //每次计算结束的时间
      if (train_result == 0){
        println("Model training completed")
      }else{
        println("Error in model training")
      }
      println((endtime-starttime)/1000000000+"秒")  //每次运算的用时

      println("=====================saveModel=======================")
      savemodel_result = abnormal.saveModel(modelFilePath)
      if (savemodel_result == 0){
        println("The training model has been saved successfully")
      }else{
        println("Training model saving failed")
      }

    }

     println("================loadmodel==============")
     loadmodel_result = abnormal.loadModel(modelFilePath)
     if (loadmodel_result == 0) {
       println("Model loaded successfully")
       if (is_evaluation.toInt == 1){

         //根据模型评估标准，计算准确率
         println("Start model evaluation.")
         val starttime=System.nanoTime  //每次计算的开始时间
         abnormal.getRMSE(testData.collect(),0)  //
         val endtime=System.nanoTime     //每次计算结束的时间
         println("模型评估耗时："+(endtime-starttime)/1000000000+"秒")  //每次运算的用时
       }

       if(is_predict.toInt == 1){
         // 实时预测模拟
         println("================predict start=================")
         for (i<- 1001 to 2000){
           val predicted_data = testData.collect().take(i).takeRight(1)
           val starttime=System.nanoTime  //每次计算的开始时间
           val rickevl = abnormal.predict(predicted_data)  //循环读入一个数据
           val endtime=System.nanoTime     //每次计算结束的时间
           println((endtime-starttime)/1000000+"豪秒")  //每次运算的用时
         }
     } else {
       println("")
     }
   }else {
       println("No corresponding training model.")
     }
   }



}
