package com.bonc.models

import java.io._

import com.bonc.interfaceRaw.IModel
import org.apache.spark.sql.types.DoubleType

//import com.bonc.Interface.IModel
import com.bonc.serviceImpl.MyObject
import net.sf.json.JSONObject
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{DenseVector, Vector}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util._

/**
  * 原张娜class
//class  FaultD extends java.io.Serializable {
////  val conf = new SparkConf().set("spark.driver.memory", "2g").set("spark.executor.memory", "2g").setAppName("KAD").setMaster("spark://172.16.32.139:7077")
////  @warn不用在内部加
////  val conf = new SparkConf().set("spark.driver.memory", "2g").set("spark.executor.memory", "2g").setAppName("FaultD").setMaster("local")
//  @transient
//  val spark = SparkSession.builder()
////  .config(conf)
//  .getOrCreate()
//  @transient
//  val sc = SparkContext
//    .getOrCreate()
////    .getOrCreate(conf)
//  //val sqlContext = new SQLContext(sc)
//
//  case class Params(tvModel:Array[Array[Row]],params:String,affinity:Double)
//
//  var tvModel: Array[Array[Row]] = null
//  var inputColIndex: Array[Int] = null
//  var labelColIndex: Array[Int] = null
//  var w: Long = 0
//  var threshold: Long = 0
//  var testDFGlobal: DataFrame = spark.emptyDataFrame
//  //var MinMax: StandardScalerModel = null
//  var MinMax: MinMaxScalerModel = null
//  var jsonStr = ""
//  var  beta:Double = 0.0
//
//  def setParam(paramStr: String): Integer = {
//    println("inside the seram")
//    //把指定目录文件里面的json串取出来
//    //val param = spark.read.json(paramStr).toJSON.select("value").collect().head(0).toString
//    //    val ps = Source.fromFile(param).mkString
//    //println(param)
//    jsonStr = paramStr
//    val paramJson = JSONObject.fromObject(paramStr)
//    println("read the json")
//    val inputcol = paramJson.getJSONArray("inputColIndex").toArray.map(_.toString.toInt)
//    val labelcol = paramJson.getJSONArray("labelColIndex").toArray.map(_.toString.toInt)
//    val w1 = paramJson.getInt("w")
//    val threshold1 = paramJson.getInt("threshold")
//    inputColIndex = inputcol
//    labelColIndex = labelcol
//    w = w1
//    threshold = threshold1
//    0
//  }
//
//  def train(inputData: Array[Row], inputColIndex1: Array[Integer], labelColIndex1: Array[Integer]): Integer = {
//    try {
//      println("this is inside the fun train")
//      val inputRows = inputData.asInstanceOf[Array[Row]]
//      val schema = inputRows(0).schema
//      val rdd = spark.sparkContext.parallelize(inputRows,1)
//      var df_org = spark.createDataFrame(rdd, schema).repartition(1)
//      println(s"df_org.count();${df_org.count()}")
//      df_org.cache()
//      val inputcol_new = new ArrayBuffer[String](inputColIndex.length)
//      for (i <- 0 until inputColIndex.length)
//        inputcol_new += df_org.columns(inputColIndex(i))
//      val inputcol = inputcol_new.toArray
//      //目标变量列名
//      val inputcol_label = df_org.columns(labelColIndex(0))
//      df_org.printSchema()
//      val vecDF: DataFrame = new VectorAssembler().setInputCols(inputcol).setOutputCol("features").transform(df_org)
//      vecDF.cache()
//      val scaler = new MinMaxScaler()
//        .setInputCol("features")
//        .setOutputCol("scaledFeatures")
//      val scalerModel = scaler.fit(vecDF)
//      MinMax = scalerModel
//      val scaledData = scalerModel.transform(vecDF)
//
//      //选取自变量和因变量列，并新增一个id列
//      //找出目标值大于阈值的点
//      val newDF = scaledData.withColumn("id", monotonically_increasing_id()).withColumnRenamed(inputcol_label, "label_values")
//      newDF.cache()
//      val a = ArrayBuffer[DataFrame]()
//      //找出故障数据记录
//      val b = newDF.filter(newDF("label_values") > threshold && newDF("id") > (w.asInstanceOf[Long]  - 1))
//      val bRow = b.collect()
//      val columns = inputcol.union(Array("label_values"))
//      for (row <- bRow) {
//        val thisID = row.getAs[Long]("id")
//        val filteredDF = newDF.filter(newDF("id") >= (thisID - w ) && newDF("id") <= (thisID -1))
//        //val maxValue = filteredDF.agg(max("label_values")).head().getDouble(0)
//        a.append(filteredDF.select(columns.map(filteredDF(_)): _*))
//      }
//      println(s"a.length:${a.length}")
//      //筛选满足条件的抗体，抗体中不能含有故障数据
//      val tv=a.toArray.map(row=>row.collect).map(row=>
//        (row,row.map(ele=>ele.getAs[Double]("label_values")))).
//        map(row=>(row._1,row._2.max)).
//        filter(_._2 <= threshold).
//        map(row => row._1)
//      // val tv=a.toArray.map(row=>row.collect())
//      println(s"a.length:${a.length}")
//      a(1).show(40,truncate = false)
//      tvModel = tv
//      println(s"tvModel.length:${tvModel.length}")
//      //计算抗体间的差异度阈值
//      val schema1=tvModel(0)(0).schema
//      //把每个抗体都变成dataframe形式
//      val tvModelN=tvModel.map(x=>spark.sparkContext.parallelize(x)).map(row=>spark.createDataFrame(row, schema1))
//      val antibodies=tvModelN.map(row=>new VectorAssembler().setInputCols(inputcol).setOutputCol("features").transform(row)).
//        map(row=>minmax(row)).
//        map(row=>vector2dataframe(row)).
//        map(row=>row.collect().map(ele=>ele.toSeq.toArray.map(x=>x.asInstanceOf[Double])))
//      //计算抗体与抗体之间的差异度
//      val affinityMatrix =antibodies.map(row=>antibodies.map(ele => MyObject.dtw(ele, row, "EuclideanDistance")))//ManhattanDistance
//      val affinity=affinityMatrix.map { ele => ele.filter(_ != 0.0).min
//      }.max
//      beta = affinity-2
//
//      0
//    }
//    catch {
//      case e: Exception =>
//        e.printStackTrace()
//        -1
//    }
//  }
//  def saveModel(modelFilePath: String): Integer = {
//    MinMax.write.overwrite().save(modelFilePath)
//    var bos = new FileOutputStream(modelFilePath+"ci")
//    var oos = new ObjectOutputStream(bos)
//    val params1 = Params(tvModel,jsonStr,beta)
//    oos.writeObject(params1)
//    oos.flush()
//    oos.close()
//    bos.flush()
//    bos.close()
//    0
//  }
//
//
//  def loadModel(modelFilePath: String): Integer = {
//    //tvModel=spark.read.option("header",true).option("inferSchema", true).csv(modelFilePath)
//    // MinMax=StandardScalerModel.load(modelFilePath)
//    MinMax=MinMaxScalerModel.load(modelFilePath)
//    var bis = new FileInputStream(modelFilePath+"ci" )
//    var ois = new ObjectInputStream(bis)
//    //tvModel = ois.readObject.asInstanceOf[Array[Array[Row]]]
//    val params_loaded = ois.readObject.asInstanceOf[Params]
//    setParam(params_loaded.params)
//    tvModel=params_loaded.tvModel
//    beta=params_loaded.affinity
//    //MinMax = ois.readObject.asInstanceOf[MinMaxScalerModel]
//    ois.close()
//    bis.close()
//    println(s"tvmodel:${tvModel.length}")
//    0
//  }
//  def predict(inputData: Array[Row]): Array[Row] = {
//    val inputRows = inputData.asInstanceOf[Array[Row]]
//    val schema = inputRows(0).schema
//    val schema1=tvModel(0)(0).schema
//    val rdd = spark.sparkContext.parallelize(inputRows,1)
//    val df_org = spark.createDataFrame(rdd, schema)
//
//    val tvModel1=tvModel.map(x=>spark.sparkContext.parallelize(x)).map(row=>spark.createDataFrame(row, schema1))
//    //把每个抗体都变成Array[Array[Double]]形式
//
//    val inputcol_new = new ArrayBuffer[String](inputColIndex.length)
//    for (i <- 0 until inputColIndex.length)
//      inputcol_new += df_org.columns(inputColIndex(i))
//    val inputcol = inputcol_new.toArray
//    val inputcol_label = df_org.columns(labelColIndex(0))
//    val newDF = df_org.withColumn("id", monotonically_increasing_id()).withColumnRenamed(inputcol_label, "label_values").cache()
//    val columns = inputcol.union(Array("label_values"))
//
//    //把每个抗体都变成Array[Array[Double]]形式
//    val antibodies=tvModel1.map(row=>new VectorAssembler().setInputCols(inputcol).setOutputCol("features").transform(row)).
//      map(row=>minmax(row)).
//      map(row=>vector2dataframe(row)).
//      map(row=>row.collect().map(ele=>ele.toSeq.toArray.map(x=>x.asInstanceOf[Double])))
//
//    try{
//      val Dcount=newDF.count()
//      val a = ArrayBuffer[DataFrame]()
//      if (Dcount>w) {
//        println("Dcount>w")
//        val b = newDF.filter(newDF("id") >= (w.asInstanceOf[Long] - 1))
//        val bRow = b.collect()
//        //val real = ArrayBuffer[Double]()
//        for (row <- bRow) {
//          val thisID = row.getAs[Long]("id")
//          // real.append(thisID.toDouble)
//          val filteredDF = newDF.filter(newDF("id") >= (thisID - w) && newDF("id") <= (thisID - 1))
//          a.append(filteredDF.select(columns.map(filteredDF(_)): _*))
//        }
//      }
//      else if(Dcount==w){
//        a.append(newDF)
//      }
//      else if(Dcount < w){
//        throw new ArithmeticException("the number of predict data is less than window's length")
//      }
//      //      else if(Dcount < w){
//      //        println("the data's number is less than windows")
//      //        }
//      //        }
//
//      //用map操作计算抗原和抗体间的差异度
//      //val antigens = sc.parallelize(a.toArray.map(row => minmax(VectorAssembler(row,inputcol))).map(row => DataFrame2String(row)))
//      //用map操作计算抗原和抗体间的差异度
//      //把抗原进行和抗体同样的操作
//      val antigens=sc.parallelize(
//        a.toArray.map(row=>new VectorAssembler().setInputCols(inputcol).setOutputCol("features").transform(row)).
//          map(row=>minmax(row)).
//          map(row=>vector2dataframe(row)).
//          map(row=>row.collect().map(ele=>ele.toSeq.toArray.map(x=>x.asInstanceOf[Double])))
//      )
//
//      //val antigens = sc.parallelize(a.toArray.map(row => minmax(VectorAssembler(row,inputcol))).map(row => DataFrame2String(row)),10)
//      println("+++++++++++++++++++++++++++")
//      val distancevalues = antigens.map(x => antibodies.map(ele => MyObject.dtw(ele, x, "EuclideanDistance")).min) //EuclideanDistance|ManhattanDistance
//      distancevalues.map(x => x < beta).foreach(println)
//      distancevalues.foreach(println)
//      val predict = distancevalues.map(x => x < beta).collect
//      //Row(distancevalues)
//      val confidence= distancevalues.map{x=>
//        val confidence = 1-x/beta
//        if (confidence>0){
//          val confidencenew=confidence+ 0.4
//          if (confidencenew < 1.0){confidencenew}
//          else {0.95}}
//        else {0}}.collect()
//      val tt = distancevalues.map(x =>  x < beta).collect()
//      //val tt = distancevalues.map(x => Row( x < beta)).collect()
//      val result=(tt.zip(confidence)).map{row=>Row(row._1,row._2)}
//      result.foreach(x=>println(x))
//      result
//    }}
//
//  def getRMSE(ts: Array[Row], integer: Integer): java.lang.Double = {
//    val inputRows = ts.asInstanceOf[Array[Row]]
//    val schema = inputRows(0).schema
//    val schema1=tvModel(0)(0).schema
//    val tvModel1=tvModel.map(x=>spark.sparkContext.parallelize(x)).map(row=>spark.createDataFrame(row, schema1))
//
//    //val tvModel1=tvModel.map(x=>spark.sparkContext.parallelize(x,1)).map(row=>spark.createDataFrame(row, schema1))
//    tvModel1(1).printSchema()
//    val rdd = spark.sparkContext.parallelize(inputRows,1)
//    val df_org = spark.createDataFrame(rdd, schema).repartition(1)
//    val inputcol_new = new ArrayBuffer[String](inputColIndex.length)
//    for (i <- 0 until inputColIndex.length)
//      inputcol_new += df_org.columns(inputColIndex(i))
//    val inputcol = inputcol_new.toArray
//    val inputcol_label = df_org.columns(labelColIndex(0))
//    val newDF = df_org.withColumn("id", monotonically_increasing_id()).withColumnRenamed(inputcol_label, "label_values").cache()
//    newDF.show(40,false)
//    val newDF1: DataFrame = new VectorAssembler().setInputCols(inputcol).setOutputCol("features").transform(newDF)
//    newDF1.cache()
//    val a = ArrayBuffer[DataFrame]()
//    val b = newDF1.filter(newDF1("id") >= w)
//    val bRow = b.collect()
//    val real = ArrayBuffer[Array[Row]]()
//    val columns = inputcol.union(Array("label_values"))
//    for (row <- bRow) {
//      val thisID = row.getAs[Long]("id")
//      val Preal=newDF1.filter(newDF1("id") >= (thisID) && newDF("id") <= (thisID+w-1)).select("label_values").collect()
//      real.append(Preal)
//      val filteredDF = newDF1.filter(newDF1("id") >= (thisID - w) && newDF("id") <= (thisID - 1))
//      a.append(filteredDF.select(columns.map(filteredDF(_)): _*))
//    }
//    //计算抗原对应后时间窗口内的目标值
//    val realc=real.toArray.map(row=>row.
//      map(ele=>ele.toSeq.toArray.
//        map(x=>x.asInstanceOf[Double]))).
//      map(row=>row.flatMap(X=>X))
//    //在数据里面筛出目标值大于阈值，并且抗原所在数据后有一个完整时间窗口数据,并把对应时间窗口内的预测变量的实际值求出来，
//    // 目前所用的方法是只要固定时间窗口内有大于等于1条故障，则认为该记录是故障
//    val tv=a.toArray.map(row=>(row,row.select("label_values").collect().map(x=>x.getAs[Double]("label_values")))).
//      map(row=>(row._1,row._2.max)).zip(realc).
//      map(row=>(row._1._1,row._1._2,row._2,row._2.size)).
//      filter(_._4==w).filter(_._2<=threshold).map(row=>(row._1,row._2,row._3.map(x=>x>threshold).contains(true))).map(x=>(x._1,x._3))
//
//    //把抗体编程Array[Array[Double]]形式的
//    val antibodies=tvModel1.map(row=>new VectorAssembler().setInputCols(inputcol).setOutputCol("features")
//      .transform(row)).
//      map(row=>minmax(row)).
//      map(row=>vector2dataframe(row)).
//      map(row=>row.collect().map(ele=>ele.toSeq.toArray.map(x=>x.asInstanceOf[Double])))
//
//
//    //用map操作计算抗原和抗体间的差异度
//    //val antigens = sc.parallelize(a.toArray.map(row => minmax(VectorAssembler(row,inputcol))).map(row => DataFrame2String(row)))
//    //用map操作计算抗原和抗体间的差异度
//    val antigens=sc.parallelize(
//      tv.map(x=>x._1).map(row=>new VectorAssembler().setInputCols(inputcol).setOutputCol("features").transform(row)).
//        map(row=>minmax(row)).
//        map(row=>vector2dataframe(row)).
//        map(row=>row.collect().map(ele=>ele.toSeq.toArray.map(x=>x.asInstanceOf[Double])))
//    )
//    //计算抗原可抗体的差异度
//    //val distancevalues = antigens.map(x => antibodies.map(ele => MyObject.dtw(ele, x, "ManhattanDistance")).min)
//    val distancevalues = antigens.map(x => antibodies.map(ele => MyObject.dtw(ele, x, "EuclideanDistance")).min)
//    distancevalues.collect().foreach(println)
//    //计算置信度
//    val confidence= distancevalues.map{x=>
//      val confidence = 1-x/beta
//      if (confidence>0){
//        val confidencenew=confidence+ 0.3
//        if (confidencenew < 1.0){confidencenew}
//        else {0.95}}
//      else {0}}.collect()
//    //println(s"confidence.filter(_>=0.3).size:${confidence.filter(_>=0.3).size}")
//
//
//    val tt = distancevalues.map(x =>  x < beta).collect()
//
//    //val tt = distancevalues.map(x => Row( x < beta)).collect()
//    //把预测值和置信度结合起来
//    val result=(tt.zip(confidence)).map{row=>Row(row._1,row._2)}
//
//    //val beta=10
//    //distancevalues.map(x=>x<beta).foreach(println)
//    result.foreach(println)
//
//    // val predictboolean = distancevalues.map(x => x < beta).collect()
//    //结合置信度判断
//    //    val predictboolean = distancevalues.collect.zip(confidence).
//    //      map(row=>if (row._1<beta && row._2>0.66) true else false)
//    val predictboolean = distancevalues.collect.zip(confidence).
//      map(row=>if (row._1<beta && row._2>0) true else false)
//
//
//    // val realboolean = real.toArray.map(x => x >threshold) //布尔值
//    val merge = sc.parallelize(predictboolean.zip(tv.map(x=>x._2))).countByValue()
//    //结合置信度
//    val reald=tv.map(x=>x._2)
//    val realAcon=reald.zip(confidence)
//    //把真实值、预测值和置信度结合起来
//    val realApred=reald.zip(predictboolean).zip(confidence)
//    println("++++++++++++++++++++++++==")
//    //打印误报的那部分置信度
//    realApred.filter(_._1==(false,true)).foreach(println)
//    println("_________________________")
//    realAcon.filter(_._1==true).foreach(println)
//    //sc.parallelize(merge.map(x=>Row(x))).countByValue()
//    val TP = merge.getOrElse((true, true), 0).hashCode()
//    val FP = merge.getOrElse((true, false), 0).hashCode()
//    val FN = merge.getOrElse((false, true), 0).hashCode()
//    val TN = merge.getOrElse((false, false), 0).hashCode()
//    val fp_rate = FP / (predictboolean.count(_ == true) + 0.01)
//    val fn_rate = FN / (reald.count(_ ==true)+0.01)
//    println(s"fp_rate:${fp_rate}")
//    println(s"fn_rate:${fn_rate}")
//    println(s"TP:${TP}")
//    println(s"FN:${FN}")
//    println(s"FP:${FP}")
//    println(s"TN:${TN}")
//    println(s"realboolean :${reald.count(_ == true)}")
//
//
//
//    //distancevalues.foreach(println)
//    //val tt= distancevalues.map(x=>Row(x)).collect()
//
//    1.0
//  }
//
//
//  def VectorAssembler(inputdata: DataFrame, inputcol: Array[String]): DataFrame = {
//    val newDF = new VectorAssembler().setInputCols(inputcol).setOutputCol("features").transform(inputdata)
//    newDF
//  }
//
//  def minmax(inputdata: DataFrame): DataFrame = {
//    val newDF = MinMax.transform(inputdata).select("scaledFeatures")
//    newDF
//  }
//  def DataFrame2String(inputdata: DataFrame): String = {
//    val n = inputdata.first.getAs[org.apache.spark.ml.linalg.DenseVector](0).size
//    val vecToSeq = udf((v: DenseVector) => v.toArray)
//    val exprs = (0 until n).map(i => col("_tmp").getItem(i).alias(s"f$i"))
//    import spark.implicits._
//    val output = inputdata.select(vecToSeq(col("scaledFeatures")).alias("_tmp")).select(exprs: _*)
//      .map(row => row.mkString(",")).collect().mkString(";")
//    val output1 = inputdata.select(vecToSeq(col("scaledFeatures")).alias("_tmp")).select(exprs: _*)
//    output
//
//  }
//  def vector2dataframe(inputdata: DataFrame): DataFrame = {
//    val n = inputdata.first.getAs[Vector](0).size
//    val vecToSeq = udf((v: DenseVector) => v.toArray)
//    val exprs = (0 until n).map(i => col("_tmp").getItem(i).alias(s"f$i"))
//    val output = inputdata.select(vecToSeq(col("scaledFeatures")).alias("_tmp")).select(exprs: _*)
//    output
//  }
//
//  def mutate(At1:Array[Array[Double]],At2:Array[Array[Double]]):Array[Array[Double]]={
//    val a=2*(0.75* (new Random()).nextDouble()+0.25)
//    println(s"a:${a}")
//    val b=2*(( new Random).nextDouble()-0.5)
//    println(s"b:${b}")
//    val antibody3=(At1.zip(At2)).map{row=>(row._1.zip(row._2)).map(x=>a*x._1+b*(x._1-x._2))}
//    antibody3
//  }
//
//}
*/






  //原张娜class
class  FaultD[T] extends IModel[T] with  java.io.Serializable {
//  val conf = new SparkConf().set("spark.driver.memory", "2g").set("spark.executor.memory", "2g").setAppName("KAD").setMaster("spark://172.16.32.139:7077")
//  @warn不用在内部加
//  val conf = new SparkConf().set("spark.driver.memory", "2g").set("spark.executor.memory", "2g").setAppName("FaultD").setMaster("local")
  @transient
  val spark = SparkSession.builder()
//  .config(conf)
  .getOrCreate()
@transient
  val sc = SparkContext
    .getOrCreate()
//    .getOrCreate(conf)
  //val sqlContext = new SQLContext(sc)

  case class Params(tvModel:Array[Array[Row]],params:String,affinity:Double)

  var tvModel: Array[Array[Row]] = null
  var inputColIndex: Array[Int] = null
  var labelColIndex: Array[Int] = null
  var w: Long = 0
  var threshold: Long = 0
  var testDFGlobal: DataFrame = spark.emptyDataFrame
  //var MinMax: StandardScalerModel = null
  var MinMax: MinMaxScalerModel = null
  var jsonStr = ""
  var  beta:Double = 0.0

  def setParam(paramStr: String): Integer = {
    println("inside the seram")
    //把指定目录文件里面的json串取出来
    //val param = spark.read.json(paramStr).toJSON.select("value").collect().head(0).toString
    //    val ps = Source.fromFile(param).mkString
    //println(param)
    jsonStr = paramStr
    val paramJson = JSONObject.fromObject(paramStr)
    println("read the json")
    val inputcol = paramJson.getJSONArray("inputColIndex").toArray.map(_.toString.toInt)
    val labelcol = paramJson.getJSONArray("labelColIndex").toArray.map(_.toString.toInt)
    val w1 = paramJson.getInt("w")
    val threshold1 = paramJson.getInt("threshold")
    inputColIndex = inputcol
    labelColIndex = labelcol
    w = w1
    threshold = threshold1
    0
  }

  def train(inputData: Array[T with Object], inputColIndex1: Array[Integer], labelColIndex1: Array[Integer]): Integer = {
    try {
      println("this is inside the fun train")
      val inputRows = inputData.asInstanceOf[Array[Row]]
      val schema = inputRows(0).schema
      val rdd = spark.sparkContext.parallelize(inputRows,1)
      var df_org = spark.createDataFrame(rdd, schema).repartition(1)
      println(s"df_org.count();${df_org.count()}")
      df_org.cache()
      val inputcol_new = new ArrayBuffer[String](inputColIndex.length)
      for (i <- 0 until inputColIndex.length)
        inputcol_new += df_org.columns(inputColIndex(i))
      val inputcol = inputcol_new.toArray
      //目标变量列名
      val inputcol_label = df_org.columns(labelColIndex(0))
      df_org.printSchema()

      //@cl:接收double类型
      for (column <- df_org.columns) {
        df_org = df_org.withColumn(column, col(column).cast(DoubleType))
      }

      val vecDF: DataFrame = new VectorAssembler().setInputCols(inputcol).setOutputCol("features").transform(df_org)
      vecDF.cache()
      val scaler = new MinMaxScaler()
        .setInputCol("features")
        .setOutputCol("scaledFeatures")
      val scalerModel = scaler.fit(vecDF)
      MinMax = scalerModel
      val scaledData = scalerModel.transform(vecDF)

      //选取自变量和因变量列，并新增一个id列
      //找出目标值大于阈值的点
      val newDF = scaledData.withColumn("id", monotonically_increasing_id()).withColumnRenamed(inputcol_label, "label_values")
      newDF.cache()
      val a = ArrayBuffer[DataFrame]()
      //找出故障数据记录
      val b = newDF.filter(newDF("label_values") > threshold && newDF("id") > (w.asInstanceOf[Long]  - 1))
      val bRow = b.collect()
      val columns = inputcol.union(Array("label_values"))
      for (row <- bRow) {
        val thisID = row.getAs[Long]("id")
        val filteredDF = newDF.filter(newDF("id") >= (thisID - w ) && newDF("id") <= (thisID -1))
        //val maxValue = filteredDF.agg(max("label_values")).head().getDouble(0)
        a.append(filteredDF.select(columns.map(filteredDF(_)): _*))
      }
      println(s"a.length:${a.length}")
      //筛选满足条件的抗体，抗体中不能含有故障数据
      val tv=a.toArray.map(row=>row.collect).map(row=>
        (row,row.map(ele=>ele.getAs[Double]("label_values")))).
        map(row=>(row._1,row._2.max)).
        filter(_._2 <= threshold).
        map(row => row._1)
      // val tv=a.toArray.map(row=>row.collect())
      println(s"a.length:${a.length}")
//      a(1).show(40,truncate = false)
      tvModel = tv
      println(s"tvModel.length:${tvModel.length}")
      //计算抗体间的差异度阈值
      val schema1=tvModel(0)(0).schema
      //把每个抗体都变成dataframe形式
      val tvModelN=tvModel.map(x=>spark.sparkContext.parallelize(x)).map(row=>spark.createDataFrame(row, schema1))
      val antibodies=tvModelN.map(row=>new VectorAssembler().setInputCols(inputcol).setOutputCol("features").transform(row)).
        map(row=>minmax(row)).
        map(row=>vector2dataframe(row)).
        map(row=>row.collect().map(ele=>ele.toSeq.toArray.map(x=>x.asInstanceOf[Double])))
      //计算抗体与抗体之间的差异度
      val affinityMatrix =antibodies.map(row=>antibodies.map(ele => MyObject.dtw(ele, row, "EuclideanDistance")))//ManhattanDistance
      val affinity=affinityMatrix.map { ele => ele.filter(_ != 0.0).min
      }.max
      beta = affinity-2

      0
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        -1
    }
  }
  def saveModel(modelFilePath: String): Integer = {
    MinMax.write.overwrite().save(modelFilePath)
    var bos = new FileOutputStream(modelFilePath+"ci")
    var oos = new ObjectOutputStream(bos)
    val params1 = Params(tvModel,jsonStr,beta)
    oos.writeObject(params1)
    oos.flush()
    oos.close()
    bos.flush()
    bos.close()
    0
  }


  def loadModel(modelFilePath: String): Integer = {
    //tvModel=spark.read.option("header",true).option("inferSchema", true).csv(modelFilePath)
    // MinMax=StandardScalerModel.load(modelFilePath)
    MinMax=MinMaxScalerModel.load(modelFilePath)
    var bis = new FileInputStream(modelFilePath+"ci" )
    var ois = new ObjectInputStream(bis)
    //tvModel = ois.readObject.asInstanceOf[Array[Array[Row]]]
    val params_loaded = ois.readObject.asInstanceOf[Params]
    setParam(params_loaded.params)
    tvModel=params_loaded.tvModel
    beta=params_loaded.affinity
    //MinMax = ois.readObject.asInstanceOf[MinMaxScalerModel]
    ois.close()
    bis.close()
    println(s"tvmodel:${tvModel.length}")
    0
  }
  def predict(inputData: Array[T with Object]): Array[T with Object] = {
    val inputRows = inputData.asInstanceOf[Array[Row]]
    val schema = inputRows(0).schema
    val schema1=tvModel(0)(0).schema
    val rdd = spark.sparkContext.parallelize(inputRows,1)
    var df_org = spark.createDataFrame(rdd, schema)

    val arr = df_org.columns.take(df_org.columns.length-1).map(x=>col(x))
    df_org = df_org.select(arr:_*)
    //@cl:接收double类型
    for (column <- df_org.columns) {
      df_org = df_org.withColumn(column, col(column).cast(DoubleType))
    }


    val tvModel1=tvModel.map(x=>spark.sparkContext.parallelize(x)).map(row=>spark.createDataFrame(row, schema1))
    //把每个抗体都变成Array[Array[Double]]形式

    val inputcol_new = new ArrayBuffer[String](inputColIndex.length)
    for (i <- 0 until inputColIndex.length)
      inputcol_new += df_org.columns(inputColIndex(i))
    val inputcol = inputcol_new.toArray
    val inputcol_label = df_org.columns(labelColIndex(0))
    val newDF = df_org.withColumn("id", monotonically_increasing_id()).withColumnRenamed(inputcol_label, "label_values").cache()
    val columns = inputcol.union(Array("label_values"))

    //把每个抗体都变成Array[Array[Double]]形式
    val antibodies=tvModel1.map(row=>new VectorAssembler().setInputCols(inputcol).setOutputCol("features").transform(row)).
      map(row=>minmax(row)).
      map(row=>vector2dataframe(row)).
      map(row=>row.collect().map(ele=>ele.toSeq.toArray.map(x=>x.asInstanceOf[Double])))

    try{
      val Dcount=newDF.count()
      val a = ArrayBuffer[DataFrame]()
      if (Dcount>w) {
        println("Dcount>w")
        val b = newDF.filter(newDF("id") >= (w.asInstanceOf[Long] - 1))
        val bRow = b.collect()
        //val real = ArrayBuffer[Double]()
        for (row <- bRow) {
          val thisID = row.getAs[Long]("id")
          // real.append(thisID.toDouble)
          val filteredDF = newDF.filter(newDF("id") >= (thisID - w) && newDF("id") <= (thisID - 1))
          a.append(filteredDF.select(columns.map(filteredDF(_)): _*))
        }
      }
      else if(Dcount==w){
        a.append(newDF)
      }
      else if(Dcount < w){
        throw new ArithmeticException("the number of predict data is less than window's length")
      }
      //      else if(Dcount < w){
      //        println("the data's number is less than windows")
      //        }
      //        }

      //用map操作计算抗原和抗体间的差异度
      //val antigens = sc.parallelize(a.toArray.map(row => minmax(VectorAssembler(row,inputcol))).map(row => DataFrame2String(row)))
      //用map操作计算抗原和抗体间的差异度
      //把抗原进行和抗体同样的操作
      val antigens=sc.parallelize(
        a.toArray.map(row=>new VectorAssembler().setInputCols(inputcol).setOutputCol("features").transform(row)).
          map(row=>minmax(row)).
          map(row=>vector2dataframe(row)).
          map(row=>row.collect().map(ele=>ele.toSeq.toArray.map(x=>x.asInstanceOf[Double])))
      )

      //val antigens = sc.parallelize(a.toArray.map(row => minmax(VectorAssembler(row,inputcol))).map(row => DataFrame2String(row)),10)
      println("+++++++++++++++++++++++++++")
      val distancevalues = antigens.map(x => antibodies.map(ele => MyObject.dtw(ele, x, "EuclideanDistance")).min) //EuclideanDistance|ManhattanDistance
      distancevalues.map(x => x < beta).foreach(println)
      distancevalues.foreach(println)
      val predict = distancevalues.map(x => x < beta).collect
      //Row(distancevalues)
      val confidence= distancevalues.map{x=>
        val confidence = 1-x/beta
        if (confidence>0){
          val confidencenew=confidence+ 0.4
          if (confidencenew < 1.0){confidencenew}
          else {0.95}}
        else {0}}.collect()
      val tt = distancevalues.map(x =>  x < beta).collect()
      //val tt = distancevalues.map(x => Row( x < beta)).collect()
      val result=(tt.zip(confidence)).map{row=>Row(row._1,row._2)}
      result.foreach(x=>println(x))
//      result
      result.asInstanceOf[Array[T with Object]]
    }}

  def getRMSE(ts: Array[T with Object], integer: Integer): java.lang.Double = {
    val inputRows = ts.asInstanceOf[Array[Row]]
    val schema = inputRows(0).schema
    val schema1=tvModel(0)(0).schema
    val tvModel1=tvModel.map(x=>spark.sparkContext.parallelize(x)).map(row=>spark.createDataFrame(row, schema1))

    //val tvModel1=tvModel.map(x=>spark.sparkContext.parallelize(x,1)).map(row=>spark.createDataFrame(row, schema1))
    tvModel1(1).printSchema()
    val rdd = spark.sparkContext.parallelize(inputRows,1)
    val df_org = spark.createDataFrame(rdd, schema).repartition(1)
    val inputcol_new = new ArrayBuffer[String](inputColIndex.length)
    for (i <- 0 until inputColIndex.length)
      inputcol_new += df_org.columns(inputColIndex(i))
    val inputcol = inputcol_new.toArray
    val inputcol_label = df_org.columns(labelColIndex(0))
    val newDF = df_org.withColumn("id", monotonically_increasing_id()).withColumnRenamed(inputcol_label, "label_values").cache()
//    newDF.show(40,false)
    val newDF1: DataFrame = new VectorAssembler().setInputCols(inputcol).setOutputCol("features").transform(newDF)
    newDF1.cache()
    val a = ArrayBuffer[DataFrame]()
    val b = newDF1.filter(newDF1("id") >= w)
    val bRow = b.collect()
    val real = ArrayBuffer[Array[Row]]()
    val columns = inputcol.union(Array("label_values"))
    for (row <- bRow) {
      val thisID = row.getAs[Long]("id")
      val Preal=newDF1.filter(newDF1("id") >= (thisID) && newDF("id") <= (thisID+w-1)).select("label_values").collect()
      real.append(Preal)
      val filteredDF = newDF1.filter(newDF1("id") >= (thisID - w) && newDF("id") <= (thisID - 1))
      a.append(filteredDF.select(columns.map(filteredDF(_)): _*))
    }
    //计算抗原对应后时间窗口内的目标值
    val realc=real.toArray.map(row=>row.
      map(ele=>ele.toSeq.toArray.
        map(x=>x.asInstanceOf[Double]))).
      map(row=>row.flatMap(X=>X))
    //在数据里面筛出目标值大于阈值，并且抗原所在数据后有一个完整时间窗口数据,并把对应时间窗口内的预测变量的实际值求出来，
    // 目前所用的方法是只要固定时间窗口内有大于等于1条故障，则认为该记录是故障
    val tv=a.toArray.map(row=>(row,row.select("label_values").collect().map(x=>x.getAs[Double]("label_values")))).
      map(row=>(row._1,row._2.max)).zip(realc).
      map(row=>(row._1._1,row._1._2,row._2,row._2.size)).
      filter(_._4==w).filter(_._2<=threshold).map(row=>(row._1,row._2,row._3.map(x=>x>threshold).contains(true))).map(x=>(x._1,x._3))

    //把抗体编程Array[Array[Double]]形式的
    val antibodies=tvModel1.map(row=>new VectorAssembler().setInputCols(inputcol).setOutputCol("features")
      .transform(row)).
      map(row=>minmax(row)).
      map(row=>vector2dataframe(row)).
      map(row=>row.collect().map(ele=>ele.toSeq.toArray.map(x=>x.asInstanceOf[Double])))


    //用map操作计算抗原和抗体间的差异度
    //val antigens = sc.parallelize(a.toArray.map(row => minmax(VectorAssembler(row,inputcol))).map(row => DataFrame2String(row)))
    //用map操作计算抗原和抗体间的差异度
    val antigens=sc.parallelize(
      tv.map(x=>x._1).map(row=>new VectorAssembler().setInputCols(inputcol).setOutputCol("features").transform(row)).
        map(row=>minmax(row)).
        map(row=>vector2dataframe(row)).
        map(row=>row.collect().map(ele=>ele.toSeq.toArray.map(x=>x.asInstanceOf[Double])))
    )
    //计算抗原可抗体的差异度
    //val distancevalues = antigens.map(x => antibodies.map(ele => MyObject.dtw(ele, x, "ManhattanDistance")).min)
    val distancevalues = antigens.map(x => antibodies.map(ele => MyObject.dtw(ele, x, "EuclideanDistance")).min)
    distancevalues.collect().foreach(println)
    //计算置信度
    val confidence= distancevalues.map{x=>
      val confidence = 1-x/beta
      if (confidence>0){
        val confidencenew=confidence+ 0.3
        if (confidencenew < 1.0){confidencenew}
        else {0.95}}
      else {0}}.collect()
    //println(s"confidence.filter(_>=0.3).size:${confidence.filter(_>=0.3).size}")


    val tt = distancevalues.map(x =>  x < beta).collect()

    //val tt = distancevalues.map(x => Row( x < beta)).collect()
    //把预测值和置信度结合起来
    val result=(tt.zip(confidence)).map{row=>Row(row._1,row._2)}

    //val beta=10
    //distancevalues.map(x=>x<beta).foreach(println)
    result.foreach(println)

    // val predictboolean = distancevalues.map(x => x < beta).collect()
    //结合置信度判断
    //    val predictboolean = distancevalues.collect.zip(confidence).
    //      map(row=>if (row._1<beta && row._2>0.66) true else false)
    val predictboolean = distancevalues.collect.zip(confidence).
      map(row=>if (row._1<beta && row._2>0) true else false)


    // val realboolean = real.toArray.map(x => x >threshold) //布尔值
    val merge = sc.parallelize(predictboolean.zip(tv.map(x=>x._2))).countByValue()
    //结合置信度
    val reald=tv.map(x=>x._2)
    val realAcon=reald.zip(confidence)
    //把真实值、预测值和置信度结合起来
    val realApred=reald.zip(predictboolean).zip(confidence)
    println("++++++++++++++++++++++++==")
    //打印误报的那部分置信度
    realApred.filter(_._1==(false,true)).foreach(println)
    println("___________________________")
    realAcon.filter(_._1==true).foreach(println)
    //sc.parallelize(merge.map(x=>Row(x))).countByValue()
    val TP = merge.getOrElse((true, true), 0).hashCode()
    val FP = merge.getOrElse((true, false), 0).hashCode()
    val FN = merge.getOrElse((false, true), 0).hashCode()
    val TN = merge.getOrElse((false, false), 0).hashCode()
    val fp_rate = FP / (predictboolean.count(_ == true) + 0.01)
    val fn_rate = FN / (reald.count(_ ==true)+0.01)
    println(s"fp_rate:${fp_rate}")
    println(s"fn_rate:${fn_rate}")
    println(s"TP:${TP}")
    println(s"FN:${FN}")
    println(s"FP:${FP}")
    println(s"TN:${TN}")
    println(s"realboolean :${reald.count(_ == true)}")



    //distancevalues.foreach(println)
    //val tt= distancevalues.map(x=>Row(x)).collect()

    1.0
  }


  def VectorAssembler(inputdata: DataFrame, inputcol: Array[String]): DataFrame = {
    val newDF = new VectorAssembler().setInputCols(inputcol).setOutputCol("features").transform(inputdata)
    newDF
  }

  def minmax(inputdata: DataFrame): DataFrame = {
    val newDF = MinMax.transform(inputdata).select("scaledFeatures")
    newDF
  }
  def DataFrame2String(inputdata: DataFrame): String = {
    val n = inputdata.first.getAs[org.apache.spark.ml.linalg.DenseVector](0).size
    val vecToSeq = udf((v: DenseVector) => v.toArray)
    val exprs = (0 until n).map(i => col("_tmp").getItem(i).alias(s"f$i"))
    import spark.implicits._
    val output = inputdata.select(vecToSeq(col("scaledFeatures")).alias("_tmp")).select(exprs: _*)
      .map(row => row.mkString(",")).collect().mkString(";")
    val output1 = inputdata.select(vecToSeq(col("scaledFeatures")).alias("_tmp")).select(exprs: _*)
    output

  }
  def vector2dataframe(inputdata: DataFrame): DataFrame = {
    val n = inputdata.first.getAs[Vector](0).size
    val vecToSeq = udf((v: DenseVector) => v.toArray)
    val exprs = (0 until n).map(i => col("_tmp").getItem(i).alias(s"f$i"))
    val output = inputdata.select(vecToSeq(col("scaledFeatures")).alias("_tmp")).select(exprs: _*)
    output
  }

  def mutate(At1:Array[Array[Double]],At2:Array[Array[Double]]):Array[Array[Double]]={
    val a=2*(0.75* (new Random()).nextDouble()+0.25)
    println(s"a:${a}")
    val b=2*(( new Random).nextDouble()-0.5)
    println(s"b:${b}")
    val antibody3=(At1.zip(At2)).map{row=>(row._1.zip(row._2)).map(x=>a*x._1+b*(x._1-x._2))}
    antibody3
  }

}
