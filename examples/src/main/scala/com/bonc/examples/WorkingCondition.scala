package com.bonc.examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by patrick on 2018/4/19.
  * 作者：齐鹏飞
  */
object WorkingCondition {

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
//    单机
    //@warn:找不到工况划分测试.csv
//    val inputPath = "\"C:\\\\Users\\\\魏永朝\\\\Desktop\\\\工况划分测试.csv\""
//    val outputFilepath = "C:\\Users\\魏永朝\\Desktop\\123"
//    val conf = new SparkConf().setMaster("local[*]").setAppName("WorkingCondition").set("spark.driver.memory","2g").set("spark.executor.memory","2g")
//    val spark = SparkSession.builder().config(conf).getOrCreate()

    //分布式下
    val inputPath = args(0)
    val outputFilepath = args(1)
//    val json_path =args(1)
//    val saveModelPath = args(2)
    val appName = "Working-Condition"
    val conf = new SparkConf().setAppName(appName).set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    @transient
    val sparkContext = new SparkContext(conf)
    @transient
    val spark = SparkSession.builder().appName(appName).getOrCreate()



    import spark.implicits._

    var df =spark.read.option("header",true).option("inferSchema",true).csv(inputPath)
    df.cache()
    df.show()
    df.printSchema()

    //设置工况条件以及其偏差
    //val boundaryCol = Array(("GT2发电机功率",5.0),("GT1发电机功率",2.0),("汽机功率",3.0))
    val boundaryCol = Array(("GT2发电机功率",20.0),("GT1发电机功率",20.0))
    //设置初始值，不设置的话默认为全局最小值。
    val startValue = Array()
    //设置连接符号
    val linkSymbol = "&"

    for(i<-boundaryCol.indices){
      var minValue = 0.0
      if (startValue.length>0){
        minValue=startValue(i)
      }else{
        minValue = df.agg(min(boundaryCol(i)._1)).head().getDouble(0)-1e-10
      }
      //自定义函数，计算当前记录的类别
      val coder:(Double=>String)=(value:Double)=>{math.ceil((value-minValue)/boundaryCol(i)._2.asInstanceOf[Double]).toInt.toString}//核心
      val sqlFunc = udf(coder)
      df =df.withColumn(s"类别${i+1}",sqlFunc(df(boundaryCol(i)._1)))
    }

    if(boundaryCol.length>1){
      val coder2:((String,String)=>String)=(str1:String,str2:String)=>{str1+linkSymbol+str2}
      val sqlFunc2 = udf(coder2)
      val arrCols = (1 to boundaryCol.length).map(num=>df(s"类别${num}"))
      df.show()
      df.printSchema()
      //如果只有两个工况条件，直接输出总类别
      df =df.withColumn("总类别",sqlFunc2(arrCols(0),arrCols(1)))
      //如果大于两个，则连接所有的类别输出
      if (arrCols.length>2){
        for(j<-2 until arrCols.length){
          df = df.withColumn("总类别",sqlFunc2($"总类别",arrCols(j)))
        }
      }
    }

    df.show()
    //保存处理的文件
    df.write.option("header",true).mode(SaveMode.Overwrite).csv(outputFilepath)

  }

}
