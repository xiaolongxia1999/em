package com.bonc.models
/**
  * Created by patrick on 2018/1/18.
  */

import java.text.SimpleDateFormat
import java.util.Date

import com.bonc.interfaceRaw.IModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
//import shapeless.T


class BoncCorrClass[T] extends IModel[T]{
  //private[this] val conf = new SparkConf().setAppName("boncCorrPackage.BoncCorrClass").setMaster("local")
  private[this] val spark = SparkSession.builder().getOrCreate()
  var m_model = null
  var m_modelParam = null
  var corrDF = spark.emptyDataFrame
  var id = ""
  println("this is outside the func")
  private[this] var timeBefore = new Date()
  private[this] var timeAfter = new Date()

  override def train(inputData: Array[T with Object],inputColIndex: Array[Integer], labelColIndex: Array[Integer]): Integer ={
    try{
      println("this is inside the fun train")
      val inputRows = inputData.asInstanceOf[Array[Row]]
      val schema = inputRows(0).schema
      val rdd = spark.sparkContext.makeRDD(inputRows)
      val df_org = spark.createDataFrame(rdd,schema)
/*
      val filePath = "D:/工业项目/corr.csv"
      val df_org = spark.read.option("header",true).csv(filePath)
      */

      val df = df_org.na.fill("0")
      df.cache()
      var filteredDF = removeZero(removeAbnormalData(df)) //去除异常的列和0值

      /*暂时不去连续重复值
      var timeBefore = new Date()
      val arr_filtered = removeRedundant(filteredDf,4)
      var timeAfter = new Date()
      println("Time consumed in this process is "+calculateTime(timeBefore,timeAfter))
      val rdd_row = sc.makeRDD(arr_filtered).map(x=>Row.fromSeq(x))
      filteredDF = spark.createDataFrame(rdd_row,df.schema)
      filteredDF.show()

       */

      val filteredRdd = filteredDF.rdd.map(row=>Row.fromSeq(row.toSeq.map(_.toString.toDouble)))
      filteredDF = spark.createDataFrame(filteredRdd,StructType(filteredDF.schema.fields.map(x=>StructField(x.name,DoubleType,x.nullable))))
      filteredDF.cache()
      timeBefore = new Date()
      /*
      val assembler = new VectorAssembler()
        .setInputCols(filteredDF.columns)
        .setOutputCol("features")

      val assembledData = assembler.transform(filteredDF)
      val corrMatrix = Correlation.corr(assembledData,"features")
      */
      val rddVector = filteredDF.rdd.map(row=>Vectors.dense(row.toSeq.toArray.map(x=>x.toString.toDouble)))//转化为vector形式
      val corrMatrix = Statistics.corr(X=rddVector,"pearson")//生成相关性矩阵
      timeAfter = new Date()
      println("Time consumed in matrix production is "+calculateTime(timeBefore,timeAfter))

      val iter = corrMatrix.rowIter
      val arr_matrix = new Array[Row](corrMatrix.numRows)
      var k =0
      while (iter.hasNext){
        arr_matrix(k)=Row.fromSeq(iter.next().toArray.toSeq)
        k+=1
      }

      val corr_rdd = spark.sparkContext.makeRDD(arr_matrix)
      corrDF = spark.sqlContext.createDataFrame(corr_rdd,StructType(filteredDF.schema.fields.map(x=>StructField(x.name,DoubleType,x.nullable))))

      timeAfter = new Date()
      println("Time consumed in proudcing correlation matrix is "+calculateTime(timeBefore,timeAfter))
      0
    }catch {
      case e:Exception=>
        e.printStackTrace()
        -1
    }

  }

  override def predict(inputData: Array[T with Object]): Array[T with Object] = {
    try{
      val col = corrDF.select(id)
      val arr = addMark(col.collect().map(_(0).toString()),corrDF.columns)  //为选定的列添加标记
      //arr.foreach(println)
      val posetive = arr.filter(_._2>0).sortBy(_._2).reverse   //对正相关系数排序
      val negative = arr.filter(_._2<=0).sortBy(_._2)   //对负相关系数排序

      val corr_desc = spark.read.option("header","true").csv("D:/工业项目/corr_desc.csv")   //读入中文和位号对应表
      val pos_chinese = translate(posetive,corr_desc)  //查找对应中文并显示
      val neg_chinese = translate(negative,corr_desc)
      Array(Row(pos_chinese,neg_chinese)).asInstanceOf[Array[T with Object]]
    }catch {
      case e:Exception=>
        e.printStackTrace()
        null
    }

  }

  override def saveModel(modelFilePath: String): Integer = {
    corrDF.write.option("header","true").mode(SaveMode.Overwrite).csv(modelFilePath)
    0
  }

  override def loadModel(modelFilePath: String): Integer = {
    val corrDF = spark.read.option("header","true").csv(modelFilePath)
    0
  }

  override def setParam(paramStr: String) : Integer = {
    val dict = spark.read.json(paramStr)
    id = dict.select("id").collect().head(0).asInstanceOf[String]
    //id = dict.asInstanceOf[Dictionary].get(id)
    0
  }

  override def getRMSE(inputData: Array[T with Object], labelColIndex: Integer): java.lang.Double = {
    1.0
  }


  private def removeZero(input:DataFrame)={ //去除含有0值的行
    timeBefore = new Date
    val df_new = input
    df_new.cache()
    val conZero =df_new.columns.map(_+"!='0'").reduce((x,y)=>x+" AND "+y)
    val filteredDF = df_new.where(conZero)
    println("number of filtered rows:"+filteredDF.count)
    timeAfter = new Date()
    println("Time consumed in the removedZero module is "+calculateTime(timeBefore,timeAfter))
    filteredDF
  }

  private def removeAbnormalData(dfInput:DataFrame)={
    timeBefore = new Date()
    val len = dfInput.count()
    var df_temp:DataFrame= dfInput
    for(col<- dfInput.columns){
      val data = dfInput.select(col).rdd.countByValue().toSeq.sortBy(_._2).reverse  //对元素重复值统计
      var isEmptyTooMuch = false
      for (count<-data if count._1(0).equals("0")){   //统计含有一定比例0值的列
        isEmptyTooMuch =(count._2*1.0/len)>0.2
      }
      if ((data(0)._2)*1.0/len>0.6 ||isEmptyTooMuch){  //统计含有一定比例重复值的列
        println("columns need to be removed are "+col)
        df_temp = df_temp.drop(col)
      }
    }
    println("number of left columns: "+df_temp.columns.length)
    timeAfter = new Date()
    println("Time consumed in the removedAbnormalDate module is "+calculateTime(timeBefore,timeAfter))
    df_temp
  }

  private def calculateTime(date1:Date,date2:Date):String={
    val time = math.abs(date2.getTime-date1.getTime)
    val dateFormat = new SimpleDateFormat("mm:ss")
    dateFormat.format(time)
  }

  private def addMark(df_arr:Array[String],cols:Array[String])={
    val arr = new Array[Tuple2[String,Double]](df_arr.length)
    var index = 0
    for(x<-0 until(df_arr.length)){
      arr(index) = (cols(index),df_arr(index).toDouble.formatted("%.2f").toDouble)  //创建新表，添加位号
      index+=1
    }
    arr
  }

  private def translate(arr:Array[Tuple2[String,Double]],desc_table:DataFrame)={
    var arr_temp = new Array[Tuple2[String,Double]](arr.length)
    var index = 0
    for(x<-0 until(arr.length)){ //创建新表，替换位号为中文名称
      //desc_table.show(1)
      val chineseName = desc_table.where(desc_table("对照名称0")===arr(index)._1).select("对照名称1").take(1)(0)(0).toString
      arr_temp(index)=(chineseName,arr(index)._2)
      index+=1
    }
    arr_temp
  }

  private def removeRedundant(df:DataFrame, num:Int)={
    val arr = df.rdd.map(_.toSeq.map(_.toString.toDouble)).collect()
    val seq_index = indexNeedToBeRemoved(arr,num)
    val arr_filtered = new Array[Seq[Double]](arr.length-seq_index.length)
    var new_index = 0
    for(x<-0 until(arr.length) if !seq_index.contains(x)){
      arr_filtered(new_index)=arr(x)
      new_index+=1
    }
    arr_filtered
  }

  private def indexNeedToBeRemoved(arr:Array[Seq[Double]],num:Int)={
    var arr_count = new Array[Int](arr(0).length) //记录连续重复次数
    var last_element = Seq.fill(arr(0).length)(999999.999999) //记录上一
    var seq_index:Seq[Int] = Seq.empty  //输出待清除的索引
    for(index<-0 until(arr.length)){  //遍历每一行
      for(i<-0 until(arr(0).length)){ //遍历每一行中的元素
        if(arr(index)(i)==last_element(i)){ //如果重复
          arr_count(i)+=1
          if(index==arr.length-1){  //如果是最后一行，清算计数
            if(arr_count(i)>=num-1){  //如果超限
              for(j<-index-arr_count(i) to index){  //记录连续重复的索引
                seq_index = Seq.concat(seq_index,Seq(j)).distinct
                arr_count(i)=0
              }
            }else{  //未超限，清零
              arr_count(i)=0
            }
          }
        }else{  //如果不重复也进行清算
          if(arr_count(i)>=num-1){
            for(j<-index-arr_count(i)-1 to index-1){
              seq_index = Seq.concat(seq_index,Seq(j)).distinct
              arr_count(i)=0
            }
          }else{
            arr_count(i)=0
          }
        }
      }
      last_element=arr(index)
      println("the process of removeRedundant: "+index/(arr.length*1.0/100)+"%")
    }
    seq_index.distinct
  }


}
