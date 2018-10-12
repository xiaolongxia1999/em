package com.bonc.examples

import com.bonc.models.FaultD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import util.control.Breaks._

object FaultDObject {
  Logger.getLogger("org").setLevel(Level.ERROR)
  //Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {


    for(i<-0 until 10) {
      breakable {
        if (i == 3 || i == 6) {
          break
        }
        println(i)
      }
    }

      //
//
//    val FaultD = new FaultD
////    val filePath = args(0)
////    val jsonPath=args(1)
////    val dataPath=args(2)
//
//
//    //val filePath = "/usr/zn/merge_data1.csv"
//    //    val jsonPath="/usr/zn/ofault_scala2.txt"
//
//    //单机
////    val dir = "E:\\bonc\\工业第三期需求\\bonc第3期项目打包\\能源管理平台3期0720\\能源管理平台3期\\预警分析\\"
////    val filePath = dir+ "data\\merge_data1.csv"
////    val jsonPath=dir + "参数json\\ofault_scala.txt"
////    val dataPath=dir + "savepath"
//
//
//////    分布式
//    val filePath = args(0)
//    val jsonPath =args(1)
//    val dataPath = args(2)
//    val appName = "FaultD"
//    val conf = new SparkConf().setAppName(appName).set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//    @transient
//    val sparkContext = new SparkContext(conf)
//    @transient
//    val spark = SparkSession.builder().appName(appName).getOrCreate()
//
//
//
////    val filePath = "C:\\Users\\魏永朝\\Desktop\\能源管理平台3期\\预警分析\\data\\merge_data1.csv"
////    val jsonPath="C:\\Users\\魏永朝\\Desktop\\能源管理平台3期\\预警分析\\参数json\\ofault_scala.txt"
////    val dataPath="C:\\Users\\魏永朝\\Desktop\\能源管理平台3期\\预警分析\\savepath"
//
//
//    val df_org = FaultD.spark.read.option("header", true).option("inferSchema", true).csv(filePath)
//    df_org.cache()
////    val jsonPath="/usr/zn/ofault_scala2.txt"
////    val jsonPath="/home/biop/data/zntest/ofault_scala.txt"
//
//
//    val ps = Source.fromFile(jsonPath).mkString
//
//
//    FaultD.setParam(ps)
//    //
//    FaultD.train(df_org.collect().take(180000), Array(new Integer(1)), Array(new Integer(2)))
//
////    val dataPath="/home/biop/data/zntest/models"
//    FaultD.saveModel(dataPath)
//    FaultD.loadModel(dataPath)
//    FaultD.predict(df_org.collect().takeRight(40))
//    FaultD.getRMSE(df_org.collect().take(202000).takeRight(4000).take(2000),1)

  }

}
