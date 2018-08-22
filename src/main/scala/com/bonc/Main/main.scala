package com.bonc.Main

import java.io.{File, FileInputStream}
import java.util.Properties

import com.bonc.interfaceRaw.IModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, IntegerType}

//import com.bonc.Interface.IModel
//import com.bonc.models.TransferEntropyModel
import net.sf.json.JSONObject
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import com.bonc.utils.df2schemaUtils
import scala.io.Source


/**
  * Created by Administrator on 2018/7/23 0023.
  */
object main {

  def getConfig():Properties={
    val dirs  = new File("")
    val fileSep = File.separator
    val path = dirs.getAbsolutePath +fileSep + "conf"+ fileSep+"default.properties"
    ModelsFactory.init(path)
  }

  def train(shortName:String,jsonStr:String,savePath:String,trainDataRaw:Array[Row],inputColIndex:Array[Integer],outputColIndex:Array[Integer]):Unit={
    //val path = "conf/default.properties"
    val dirs  = new File("")
    val fileSep = File.separator
    val path = dirs.getAbsolutePath +fileSep + "conf"+ fileSep+"default.properties"

    println("path is:---------------------------")
    println(path)

    //加载类名，动态创建IModel实例
    val props = ModelsFactory.init(path)
    val model = ModelsFactory.produce(shortName,props)

    //训练阶段方法
    model.setParam(jsonStr)
    model.train(trainDataRaw,inputColIndex,outputColIndex)
    model.saveModel(savePath)
  }

  def predict(shortName:String,jsonStr:String,savePath:String,predictSetRaw:Array[Row]):Unit= {
//    val path = "conf/default.properties"
    val dirs  = new File("")
    val fileSep = File.separator
    val path = dirs.getAbsolutePath +fileSep + "conf"+ fileSep+"default.properties"

    //加载类名，动态创建IModel实例
    val props = ModelsFactory.init(path)
    val model = ModelsFactory.produce(shortName,props)

    model.setParam(jsonStr)
    model.loadModel(savePath)
    model.predict(predictSetRaw)
  }
//
//  def clac(shortName:String,jsonStr:String,predictSetRaw:Array[Row]):Unit= {
//    //    val path = "conf/default.properties"
//    val dirs  = new File("")
//    val fileSep = File.separator
//    val path = dirs.getAbsolutePath +fileSep + "conf"+ fileSep+"default.properties"
//
//    //加载类名，动态创建IModel实例
//    val props = ModelsFactory.init(path)
//    val model = ModelsFactory.produce(shortName,props)
//
//    model.setParam(jsonStr)
//
//    model.calc(predictSetRaw)
//  }




  def main(args: Array[String]): Unit = {



//    val conf = new SparkConf().setAppName("IModel").setMaster("local[*]")
//    val sc = new SparkContext(conf)
//    val path = "conf/default.properties"
//    val props = ModelsFactory.init(path)
//    val model = ModelsFactory.produce("TE",props)
//    println("done!")

    //单机下

//   // 异常侦测
//        val dir = "C:\\Users\\魏永朝\\Desktop\\能源管理平台3期\\异常侦测应用场景\\"
//        val path = dir+ "data\\ProcessedAbnormalDetection.csv"
//        val json_path = "conf/json_fpgrowth_train_1_test3.txt"
//        val saveModelPath=dir + "savepath"
//        val shortName = "AD"


    //相似工况
//      val dir = "C:\\Users\\魏永朝\\Desktop\\能源管理平台3期\\相似工况生产控制场景\\"
//      val path = dir+ "data\\相似工况真实场景_65.csv"
//      val json_path = "conf/相似工况分析_二拖一总负荷_1.txt"
//      val saveModelPath=dir + "savepath\\高安屯数据_0730\\FactorAnalysisModel_er_1"
//      val shortName = "FA"


// //传递熵
    val dir = "C:\\Users\\魏永朝\\Desktop\\能源管理平台3期\\根原因分析\\"
    val path = dir+ "data\\yangsDataNoTranspose.csv"
    val json_path ="conf/te_json.txt"
    val saveModelPath = dir+"savepath\\te"
    val shortName = "TE"


//
//    //张娜预警分析
//        val dir = "C:\\Users\\魏永朝\\Desktop\\能源管理平台3期\\预警分析\\"
//        val path = dir+ "data\\merge_data1.csv"
//        val json_path = "conf/ofault_scala.txt"
//        val saveModelPath=dir + "savepath"
//        val shortName = "FD"


//    //二期质量软测量
//    val dir = "D:\\bonc\\团队管理\\EnergyManagement_zn_20180727\\"
//    val path = dir+ "data\\softsense_data_new_predict.csv"
//    val json_path = "conf/json_regression.txt"
//    val saveModelPath=dir + "savepath"
//    val shortName = "RG"

//    val dir =  "C:\\Users\\魏永朝\\Desktop\\相关性分析\\"
//    val path = dir+ "data\\corr.csv"
//    val json_path ="conf/corrJson.txt"
//    val saveModelPath = dir+"savepath"
//    val shortName = "BC"




//        val stage = "train"
//
        val stage = "predict"
        //模型别名，conf/default.properties目录下设置

        val master = "local[*]"
    //    val master = "spark://172.16.32.139:7077"
        val appName = "RG"
        val conf = new SparkConf().setAppName(appName).setMaster(master).set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        @transient
        val sparkContext = new SparkContext(conf)
        @transient
        val spark = SparkSession.builder().master(master).appName(appName).getOrCreate()




//    //分布式下
//    val path = args(0)
//    val json_path =args(1)
//    val saveModelPath = args(2)
//    //值为为“train"或者"predict"——分别进入训练或预测模式
//    val stage = args(3)
//    //模型别名，conf/default.properties目录下设置
//    val shortName = args(4)
//
//    val appName = "3rd apps"
//    val conf = new SparkConf().setAppName(appName).set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//    @transient
//    val sparkContext = new SparkContext(conf)
//    @transient
//    val spark = SparkSession.builder().appName(appName).getOrCreate()
//    val parallelism = 24


    //scala的IO,本地读取
    val jsonStr = Source.fromFile(json_path).mkString//.mkString
//    var df = spark.emptyDataFrame
//   if(shortName.equalsIgnoreCase("fa") || shortName.equalsIgnoreCase("ad")){
//     df = spark.read.format("csv").option("header",true).option("inferSchema",true).load(path)
//   }else{
//     df = spark.read.format("csv").option("header",true).load(path)
//   }
     var df = spark.read.format("csv").option("header",true).load(path)

//


    println("1st:")
    df.show()
//    //训练集
//    val arrayRowTrain = df.collect()
//    val arrayRowTrainT = arrayRowTrain.map(x=>x.asInstanceOf[Row])

    //4个隐藏参数-需要在模型外部解析出来：
    val paramJson = JSONObject.fromObject(jsonStr)
    val inputColIndex = paramJson.getJSONArray("inputColIndex").toArray.map(x=>Integer.valueOf(x.toString.toInt))
    var outputColIndex = paramJson.getJSONArray("outputColIndex").toArray.map(x=>Integer.valueOf(x.toString.toInt))

    //仅传递熵有定义
    //加载类名，动态创建IModel实例
//    val pathProp = "conf/default.properties"
//    val props = ModelsFactory.init(pathProp)
    val props = this.getConfig()
//    val model:IModel[Row] = ModelsFactory.produce(shortName,props)


//    if(shortName.equalsIgnoreCase("te")){
//      val SensorForPredict = props.getProperty("SensorForPredict").toInt
//      val arrayRowPredict = df.select(df.columns(SensorForPredict)).collect()
//      val arrayRowPredictT = arrayRowPredict.map(x=>x.asInstanceOf[Row])
//    }




    if (stage.equalsIgnoreCase("train")){

      var arrayRowTrainT:Array[Row] = null

      //异常侦测训练数据处理
      if(shortName.equalsIgnoreCase("ad")){
        df = df2schemaUtils.df2schema(df)
        //df.withColumn(df.columns(0).map(x=>col(x)),)
       var mm =  df.count()*0.95
        arrayRowTrainT= df.take(mm.toInt)
          .map(x=>x.asInstanceOf[Row])
      }else if (shortName.equalsIgnoreCase("fa")){//相似工况
        //df.columns
        df = df2schemaUtils.df2schema(df)
        arrayRowTrainT = df.na.fill(0.0).filter(!_.toSeq.contains(0.0)).collect().map(x=>x.asInstanceOf[Row])
        outputColIndex = Array(df.columns.indexOf("二拖一总负荷"))


      }else{
        arrayRowTrainT = df.collect().map(x=>x.asInstanceOf[Row])
      }
      this.train(shortName,jsonStr,saveModelPath,arrayRowTrainT,inputColIndex,outputColIndex)
    }else if(stage.equals("predict")){
      var arrayRowPredictT = df.collect().map(x=>x.asInstanceOf[Row])
      //张娜计算结果太大，需要设置取40条
          if(shortName.equalsIgnoreCase("fd")){
            arrayRowPredictT = df.collect().takeRight(40).map(x=>x.asInstanceOf[Row])
              .map(x=>x.asInstanceOf[Row])
          }
      //var arrayRowPredictT = df.collect().takeRight(40).map(x=>x.asInstanceOf[Row])
          //针对传递熵的情形，因为本地测试不必用多个文件，是从原csv文件根据列号选取某指定列
          if(shortName.equalsIgnoreCase("te")){
            val SensorForPredict = props.getProperty("sensorforpredict").toInt
            arrayRowPredictT = df.select(df.columns(SensorForPredict)).collect()
                                .map(x=>x.asInstanceOf[Row])
          }
          if(shortName.equalsIgnoreCase("ad")){
            //val SensorForPredict = props.getProperty("SensorForPredict").toInt
            arrayRowPredictT = df.collect().take(10000).takeRight(1)
              .map(x=>x.asInstanceOf[Row])
          }
          if(shortName.equalsIgnoreCase("fa")){
            //val SensorForPredict = props.getProperty("SensorForPredict").toInt
            arrayRowPredictT = df.collect().take(10000).takeRight(1)
              .map(x=>x.asInstanceOf[Row])
          }
      this.predict(shortName,jsonStr,saveModelPath,arrayRowPredictT)
    }else
      println("no train and no predict!")
  }
}
