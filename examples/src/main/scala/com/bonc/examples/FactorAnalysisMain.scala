package com.bonc.examples

import java.io.File
import java.util.Properties

import breeze.plot._
import com.bonc.Interface.IModel
import com.bonc.Main.ModelsFactory
import com.bonc.models.FactorAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by patrick on 2018/4/12.
  * 作者:
  */


object FactorAnalysisMain{
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
  var train_result = -1
  var savemodel_result = -1
  var loadmodel_result = -1



  def main(args: Array[String]): Unit = {
//    val inputfile = args(0)   //参数1：输入数据路径
//    val json_path = args(1)
//    val modelFilePath = args(2)
//    val is_Training = args(3)   //参数1：输入数据路径
//    val is_evaluation = args(4)
//    val is_predict = args(5)
//    val conf = new SparkConf().setAppName("Similar_working_condtions").setMaster("spark://172.16.32.139:7077")  //biop 集群

    //单机
//    val dir = "E:\\bonc\\工业第三期需求\\bonc第3期项目打包\\能源管理平台3期0720\\能源管理平台3期\\相似工况生产控制场景\\"
//    val inputfile =dir + "data\\相似工况真实场景_2890.csv"
//    val modelFilePath = dir + "savepath\\高安屯数据_2\\FactorAnalysisModel_er_1"
//    val json_path = dir + "json\\相似工况分析_二拖一总负荷_1.txt"

    //分布式下
//    val inputfile = args(0)
//    val json_path =args(1)
//    val modelFilePath = args(2)
//    val appName = "KAD"
//    val conf = new SparkConf().setAppName(appName).set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//    @transient
//    val sparkContext = new SparkContext(conf)
//    @transient
//    val spark = SparkSession.builder().appName(appName).getOrCreate()
    val inputfile ="C:\\Users\\魏永朝\\Desktop\\能源管理平台3期\\相似工况生产控制场景\\data\\相似工况真实场景_2890.csv"
    val modelFilePath = "C:\\Users\\魏永朝\\Desktop\\能源管理平台3期\\相似工况生产控制场景\\savepath\\高安屯数据_2\\FactorAnalysisModel_er_1"
    //val param = "C:\\Users\\魏永朝\\Desktop\\能源管理平台3期\\相似工况生产控制场景\\json\\相似工况分析_二拖一总负荷_1.txt"

    //    @original
    val conf = new SparkConf().set("spark.driver.memory","2g").set("spark.executor.memory","2g").setAppName("KAD").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    var shortName = "fa"
    val dirs  = new File("")
    val fileSep = File.separator
    val path1 = dirs.getAbsolutePath +fileSep + "conf"+ fileSep

    val path = path1+"default.properties"
    val jsonfile = path1 +"相似工况分析_二拖一总负荷_1.txt"
    println("path is:---------------------------")
    println(path)
    println(jsonfile)

    //加载类名，动态创建IModel实例
    val props = ModelsFactory.init(path)
    val factor_ana = ModelsFactory.produce(shortName,props)
    val factor_ana1 = new FactorAnalysis
    //训练阶段方法





//    val factor_ana:IModel[Row] = new FactorAnalysis().asInstanceOf[IModel[Row]]
//    val inputfile ="C:\\Users\\魏永朝\\Desktop\\相似工况真实场景_2890.csv"
   // val inputfile ="C:\\Users\\魏永朝\\Desktop\\能源管理平台3期\\相似工况生产控制场景\\data\\相似工况真实场景_65.csv"


//    val inputfile ="C:\\Users\\魏永朝\\Desktop\\相似工况真实场景_29.csv"
//    val inputfile = "/usr/wyz/work_cond/相似工况真实场景_2890.csv"
    //val inputfile = "/usr/wyz/work_cond/相似工况真实场景_test.csv"

//    val modelFilePath = "C:\\Users\\魏永朝\\Desktop\\能源管理平台3期\\相似工况生产控制场景\\savepath\\高安屯数据_1\\FactorAnalysisModel_qi_1"
    //val modelFilePath = "/usr/wyz/work_cond/savepath"
    //val json_path = "C:\\Users\\魏永朝\\Desktop\\相似工况分析.txt"qi

//    val json_path = "C:\\Users\\魏永朝\\Desktop\\能源管理平台3期\\相似工况生产控制场景\\参数json_1\\相似工况分析_汽耗率_1.txt"
    //val json_path = "/usr/wyz/work_cond/相似工况分析.txt"
//    var df = spark.read.option("header",true).option("inferSchema",true).csv("C:\\Users\\魏永朝\\Desktop\\三期数据挖掘模型\\三期数据\\input_data\\相似工况\\相似工况分析数据包Filter.csv")
    // val inputfile = "usr/wyz/work_cond/ProcessedAbnormalDetection.csv"
    var df1 = spark.read.option("header",true).option("inferSchema",true).csv(inputfile)
    df1.cache()
    val df = df1.na.fill(0.0).filter(!_.toSeq.contains(0.0))
    df.cache()

    println("总样本数： "+df.count())


    var is_Training = 0//是否为训练阶段
    var is_evaluation = 1
    var is_predict = 1
    val showModels = true //是否展示模型信息
    val showCentroids = true  //是否展示工况划分信息              是否要进行训练
    val showResultsInFigure = true  //是否展示测试效果

    //本地读取
//    val param = Source.fromFile(json_path).mkString
//    val param = spark.read.json(json_path).toJSON.select("value").collect().head(0).toString
    val param = spark.read.json(jsonfile).toJSON.select("value").collect().head(0).toString
    factor_ana.setParam(param)
    factor_ana1.setParam(param)


    println("param:——————————————————————")
    println(param)
    println("factor_ana attrs——————————————————————————" )
    println(factor_ana1.labelCol)

    val label = df.columns.indexOf(factor_ana1.labelCol)
    println("label"+label)

    if(is_Training.toInt == 1){
      println("=====================train=====================")
      var starttime=System.nanoTime

      train_result= factor_ana.train(df.collect(),Array(1),Array(label))
//      train_result= factor_ana.train(df.collect(),Array(1),df.columns.indexOf(factor_ana.labelCol))
      var endtime=System.nanoTime     //每次计算结束的时间
      if (train_result == 0){
        println("Model training completed")
      }else{
        println("Error in model training")
      }
      println((endtime-starttime)/1000000000+"秒")  //每次运算的用时

      println("=====================saveModel=======================")
      savemodel_result = factor_ana.saveModel(modelFilePath)
      if (savemodel_result == 0){
        println("The training model has been saved successfully")
      }else{
        println("Training model saving failed")
      }

    }

    println("================loadmodel==============")
    loadmodel_result = factor_ana.loadModel(modelFilePath)
    if (loadmodel_result == 0) {
      println("Model loaded successfully")
      if (is_evaluation.toInt == 1){

        //根据模型评估标准，计算准确率
        println("Start model evaluation.")
        val starttime=System.nanoTime  //每次计算的开始时间
        factor_ana.getRMSE(df.collect(),0)   //进行模型评估
        val endtime=System.nanoTime     //每次计算结束的时间
        println("模型评估耗时："+(endtime-starttime)/1000000000+"秒")  //每次运算的用时
      }
      //展示模型信息，包括中心点以及对应的特征变量，和目标变量
      if(factor_ana1.modelArray.length>0 & showModels){
        val modelInfo =factor_ana1.modelArray.toArray.map{plModel:PipelineModel=>
          val labelCol = plModel.stages.last.getParam("labelCol")
          val featureCol = plModel.stages.head.asInstanceOf[VectorAssembler].getInputCols
          s"labelCol:${labelCol}, featureCol:${featureCol.mkString(",")}"
        }
        factor_ana1.centroids.map(center=>s"center:${center}").zip(modelInfo).foreach(println)//.zip()将两个合到一起
      }


      //展示工况信息（中心点），包括中心点，以及附属的数据个数。
      if(showCentroids){
        println("工况划分（聚类中心）：")
        factor_ana1.centroids.zip(factor_ana1.centroids_rows).map(x=>s"center:${x._1},elements:${x._2}").foreach(println)
      }

      if(is_predict.toInt == 1){
        // 实时预测模拟
        println("================predict start=================")
        val labelCol_predictions = new ArrayBuffer[Row]()
        val labelCol_value = new ArrayBuffer[Row]()
        for (i<- 500 to 700){
          val predicted_data = df.collect().take(i).takeRight(1)
          labelCol_value++=df.select(factor_ana1.labelCol).collect().take(i).takeRight(1)
          val starttime=System.nanoTime  //每次计算的开始时间
          labelCol_predictions++=factor_ana.predict(predicted_data)  //循环读入一个数据
          val endtime=System.nanoTime     //每次计算结束的时间
          println((endtime-starttime)/1000000+"豪秒")  //每次运算的用时
        }
        //展示最终测试结果，以线图展示。
        val test_pred = labelCol_predictions


        if(showResultsInFigure & labelCol_predictions.length>0 & test_pred.length>0){
          val f1 =Figure()
          val p = f1.subplot(0)
          val x = (0 until(labelCol_predictions.length.toInt)).toArray.map(_.toDouble)
          val y_pred = test_pred.map(row=>row.getDouble(0))
          val y_label =labelCol_value.map(row=>row.getDouble(0))
          p+=plot(x,y_pred,name = "predictions",colorcode = "r")
          p+=plot(x,y_label,name = factor_ana1.labelCol,colorcode = "b")
          p.legend=true
          p.yaxis.setAutoRangeIncludesZero(true)
          //p.ylim(0.0,1)
          //p.title(s"${}")
        }

      } else {
        println("")
      }
    }else {
      println("No corresponding training model.")
    }
  }


}
