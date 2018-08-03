package com.bonc.examples

import com.bonc.interfaceRaw.ICalc
import com.bonc.models.RiskEvaluation
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext, sql}

import scala.io.Source

/**
  * Created by patrick on 2018/4/13.
  * 作者：
  */
object RiskEvaluationMain{
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
//  val conf = new SparkConf().set("spark.driver.memory","2g").set("spark.executor.memory","2g").setAppName("RiskEvaluation").setMaster("local[*]")  //本地模式
////  val conf = new SparkConf().setAppName("RiskEvaluation").setMaster("spark://172.16.32.139:7077")  //biop 集群
//  val spark = new sql.SparkSession.Builder().config(conf).getOrCreate()

  def main(args: Array[String]): Unit = {


    //集群方式1：通过命令行传传参数
    //val inputfile = args(0)   //参数1：输入数据路径
    //val param = args(1)

    //集群方式2：
//    val inputfile = "/usr/wyz/pump/123.csv"

    //本地读取数据
//    val dir = "E:\\bonc\\工业第三期需求\\bonc第3期项目打包\\能源管理平台3期0720\\能源管理平台3期\\机泵群风险评估监控场景\\"
//    val inputfile = dir + "data\\123.csv"  //热网真实数据，有四个机泵，都是1类型
//    val param =dir + "参数json\\pump_json.txt"  //本地热网数据{'jifen_mothed':1,'Int_interval_num':60,'pump_class':[1,1,1,1]}

    //分布式下
    val inputfile = args(0)
    val param =args(1)
//    val saveModelPath = args(2)   //风险评估不需要
    val appName = "RiskEvaluation"
    val conf = new SparkConf().setAppName(appName).set("spark.serializer","org.apache.spark.serializer.KryoSerializer").setMaster("local[*]")
    @transient
    val sparkContext = new SparkContext(conf)
    @transient
    val spark = SparkSession.builder().appName(appName).getOrCreate()


//    val inputfile ="C:\\Users\\魏永朝\\Desktop\\能源管理平台3期\\机泵群风险评估监控场景\\data\\123.csv"  //热网真实数据，有四个机泵，都是1类型
//    val param ="C:\\Users\\魏永朝\\Desktop\\能源管理平台3期\\机泵群风险评估监控场景\\参数json\\pump_json.txt"  //本地热网数据{'jifen_mothed':1,'Int_interval_num':60,'pump_class':[1,1,1,1]}

//    val inputfile = "C:\\Users\\魏永朝\\Desktop\\能源管理平台3期\\机泵群风险评估监控场景\\data\\机泵风险评估数据.csv"  //模拟数据

//    val conf = new SparkConf().set("spark.driver.memory","2g").set("spark.executor.memory","2g").setAppName("RiskEvaluation").setMaster("local[*]")  //本地模式
    //  val conf = new SparkConf().setAppName("RiskEvaluation").setMaster("spark://172.16.32.139:7077")  //biop 集群
//    val spark = new sql.SparkSession.Builder().config(conf).getOrCreate()

    val df =spark.read.option("header",true).option("inferSchema",true).csv(inputfile)

    df.cache()

    val risckeval:ICalc[Row] = new RiskEvaluation().asInstanceOf[ICalc[Row]]


//    val param ="C:\\Users\\魏永朝\\Desktop\\能源管理平台3期\\机泵群风险评估监控场景\\参数json\\pump_json_2.txt"     //模拟数据
//    val param ="/usr/wyz/pump/pump_json.txt"   //集群模式

    //本地文件读取
    val paramStr = Source.fromFile(param).mkString
    risckeval.setParam(paramStr)
//    risckeval.setParam(param)

    //json文件为：{'jifen_mothed':1,'Int_interval_num':60,'pump_class':[1,1,1,1]}
    //jifen_mothed:积分方式  0-从开始起一直累加积分区间  1-设定一个固定积分长度（利用Int_interval_num），当检测次数小于Int_interval_num，逐渐累计积分区间，当超过时，积分区间遂采集时间平移
    //Int_interval_num:积分时段（采集点个数）
    //pump_class:各机泵类型（四类：1-小型电机（小于15KW的小型电动机）
                                // 2-中型电机（15KW—75KW的电动机）
                                 // 3-大型原动机（硬基础）
                                // 4-大型原动机（弹性基础）


    for(i<-1 to 60){
      val starttime=System.nanoTime  //每次计算的开始时间
      var rickevl = risckeval.calc(df.collect().take(i))  //循环读入一个数据

      /*var schema = rickevl(0).schema
      var rdd = spark.sparkContext.makeRDD(rickevl)  //转化为rdd
      var rickevl_df= spark.createDataFrame(rdd,schema).repartition(1)
      rickevl_df.show(false)*/

      val endtime=System.nanoTime     //每次计算结束的时间
      println((endtime-starttime)/1000000+"毫秒")  //每次运算的用时

    }


  }

}
