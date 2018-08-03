package com.bonc.models

import java.text.SimpleDateFormat
import java.util.Locale

import com.bonc.Interface.ICalc1
import com.bonc.interfaceRaw.ICalc
import net.sf.json.JSONObject
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by patrick on 2018/4/13.
  * 作者：齐鹏飞
  */
class RiskEvaluation[T] extends ICalc[T with Object]{  ///已修改修，接口继承为计算类型的
  private[this] val spark = SparkSession.builder().getOrCreate()
  var stdArray = Array.emptyDoubleArray
  val arr = new ArrayBuffer[Row]()
  var Int_interval_num:Int = 0
  var jifen_mothed:Int = 1
  var Pump_type =Array.emptyIntArray


  //修改传参情况，已修改，通过读入 一个文件
  override def setParam(paramStr: String): Integer = {

    try{

//      val param = spark.read.json(paramStr).toJSON.select("value").collect().head(0).toString  //读取json文件
//      //val param = Source.fromFile(paramStr).mkString
//      println(param)
//      val paramJson = JSONObject.fromObject(param)
      val paramJson = JSONObject.fromObject(paramStr)

      println(paramJson)
      Int_interval_num  = paramJson.getString("Int_interval_num").toInt
      Pump_type = paramJson.getJSONArray("pump_class").toArray.map(_.toString.toInt)
      jifen_mothed = paramJson.getString("jifen_mothed").toInt
      0
    }catch {
      case e:Exception=>
        e.printStackTrace()
        -1
    }
  }

  override def calc(inputData: Array[T with Object]): Array[T with Object] = {

    try{
      val data = inputData.asInstanceOf[Array[Row]]
      val arr1 = data  //

      println(arr1.last)
      //将最新时刻的值添加到arr中
      arr +=arr1.last

      println(jifen_mothed,Int_interval_num,Pump_type)
      if (jifen_mothed == 1){
        //Int_interval_num = 60   //积分区间设置
        //如果arr中长度大于Int_interval_num时，每由arr +=arr1.last增加一个时刻的同时，要将arr中的第一个去掉
        if (arr.length > Int_interval_num){
          arr.remove(0)
        }
      }
      println("arr.length:"+ arr.length)

      var time = 0.0
      var timeNext = 0.0
      var pumpNum=Pump_type.length  //
      println("==========",pumpNum)
      var area=new Array[Double](pumpNum)//24列的array
      //计算每一个泵随着时间变动的速度的积分。
      if(arr.length == 1) {
         for (j <- 0 until pumpNum) {
           area(j) += arr.head.getDouble(j + 1) //时间差*（a(i)^2+a(i+1)^2)/2  梯形面积和替代曲线积分
         }
      }else{
        for (i <- arr.indices) {
          if (i < arr.length - 1) {
            time = timeInSecond(arr(i).getString(0)) //开始的时间转换：将string转换成时间秒getString(0)把第一列转化成str
            timeNext = timeInSecond(arr(i + 1).getString(0)) //下一个时刻的时间转换
            for (j <- 0 until pumpNum) {
              //循环计算各个机泵的积分面积和
              //area(j) +=(timeNext-time)*math.pow(arr(i).getDouble(j+1),2)//时间差*a(i)^2   用矩形面积和替代曲线积分
              area(j) += (timeNext - time) * (math.pow(arr(i).getDouble(j + 1), 2) + math.pow(arr(i + 1).getDouble(j + 1), 2)).toDouble / 2 //时间差*（a(i)^2+a(i+1)^2)/2  梯形面积和替代曲线积分
            }
          }
        }
        //根据公式算出震动烈度

        val timePeriod = timeInSecond(arr.last.getString(0))-timeInSecond(arr.head.getString(0))  //求出整个的时间区间
        area=area.map(x=>math.sqrt(x/timePeriod))                                                 //求均值并开方

      }

      area.foreach(println)

      //比对评级标准，输出每个泵的风险评级
      //val Pump_type=Array(1,4,2,3,1,2,1,2,1,2,1,1,1,1,2,3,1,2,1,2,1,2,1,1)

      val evaluation = area.zip(Pump_type).map(x=>(x._1,evaluator(x._1,x._2)))
      //val evaluation = area.map(x=>(evaluator(x)))
      val finalEvaluation=data(0).schema.fieldNames.tail.zip(evaluation)
      finalEvaluation.foreach(println)
      finalEvaluation.map{x=>Row(x)}.asInstanceOf[Array[T with Object]]


    }catch {
      case e:Exception=>
        e.printStackTrace()
        null
    }
  }

  def evaluator(num:Double,type1:Int):Int ={

    if(type1 ==1){
      if(num<=0.71){
        return  0
      }else if(num<=1.8){
        return 1
      }else if(num<=4.5){
        return 2
      }else{
        return 3
      }

    }else if(type1 ==2){
      if(num<=1.12){
        return 0
      }else if(num<=2.8){
        return 1
      }else if(num<=7.1){
        return 2
      }else{
        return 3
      }
    }else if(type1 ==3){
      if(num<=1.8){
        return 0
      }else if(num<=4.5){
        return 1
      }else if(num<=11.2){
        return 2
      }else{
        return 3
      }
    }else if(type1 == 4) {
      if (num <= 2.8) {
        return 0
      } else if (num <= 7.1) {
        return 1
      } else if (num <= 18) {
        return 2
      } else {
        return 3
      }
    }


    //for (i<-stdArray.indices){
    //  if (num<stdArray(i)){
    //    return i
    //  }
    //}

    return stdArray.length
  }

  def timeInSecond(str:String): Double ={
    //将时间转换成数字形式
    val loc = new Locale("en")
    val fm = new SimpleDateFormat("yyyy/M/d HH:mm:ss",loc)
    val dt2 = fm.parse(str)
    dt2.getTime().toDouble/1000
  }
}
