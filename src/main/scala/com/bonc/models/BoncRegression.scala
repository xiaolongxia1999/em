package com.bonc.models

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import java.lang.Double

import com.bonc.interfaceRaw.IModel
import net.sf.json.JSONObject
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.{DecisionTreeRegressor, GBTRegressor, RandomForestRegressor}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer


class BoncRegression[T] extends IModel[T] {
//  val sc = SparkContext.getOrCreate().addJar("file:///home/biop/bca/bin/nblab/jars/test-828_1/BoncRegression_20180310.jar")
//  val spark = SparkSession.builder().config("spark.sql.warehouse.dir", "file:///").getOrCreate()
  //设置全局变量
val spark = SparkSession.builder()
  //  .config(conf)
  .getOrCreate()
  @transient
  val sc = SparkContext
    .getOrCreate()
  var tvModel: PipelineModel = null
  var inputColIndex:Array[Int]=null
  var labelColIndex:Array[Int]=null
  var testDFGlobal:DataFrame = spark.emptyDataFrame
  var trainDFGlobal:DataFrame=spark.emptyDataFrame
  var MAPE:Double=0.0



  override def setParam(paramStr: String): Integer = {
    try{
      println("inside the setparam")
      val paramJson = JSONObject.fromObject(paramStr)		//解析paramStr，生成JSONObject对象，该对象是json对象的一个封装，可以操作json对象，比如获取属性、值等。。
      println("read the json")
      //以后遇到java转scala，都先将java的对象转成String，再转成scala里的数据类型
      val inputcol = paramJson.getJSONArray("inputCol").toArray.map(_.toString.toInt)
      val labelcol = paramJson.getJSONArray("labelColIndex").toArray.map(_.toString.toInt)
      //对全局变量进行赋值
      inputColIndex=inputcol
      labelColIndex=labelcol
      println(inputColIndex)
      println("json finished")
      0
    }catch {
      case e:Exception=>
        e.printStackTrace()
        -1
    }
    0
  }

  override def train(inputData: Array[T with Object], inputColIndex1: Array[Integer], labelColIndex1: Array[Integer]): Integer = {
    try {
      println("this is inside the fun train")
      //把传入的数据变成DataFrame格式
      val inputRows = inputData.asInstanceOf[Array[Row]]
      val schema = inputRows(0).schema
      val rdd = spark.sparkContext.parallelize(inputRows)
      val df_org = spark.createDataFrame(rdd,schema)
      //把数据中的空值用0替换，并提取出不包含0的行
      val df = df_org.na.fill(0.0)
      df.cache()
      var filterDF = df.filter(!_.toSeq.contains(0.0))
      //找出自变量所在列的列名
      var inputcol_new = new ArrayBuffer[String](inputColIndex.length)
      for (i <- 0 until inputColIndex.length)
        inputcol_new += df.columns(inputColIndex(i))
      val inputcol = inputcol_new.toArray[String]
      //把数据类型变成Double
      for (column<-filterDF.columns){
        filterDF = filterDF.withColumn(column,col(column).cast(DoubleType))
      }
      filterDF.show(20)
      filterDF.printSchema()
      //提取特征指标数据,将多列数据转化为单列的向量列
      var vecDF: DataFrame = new VectorAssembler().setInputCols(inputcol).setOutputCol("features").transform(filterDF)
      vecDF.show(20)
      //目标变量列名
      val labelcol = df.columns(labelColIndex(0))
      //连续四个数据不同则视为连续值
      val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(4)
        .fit(vecDF)
      //把数据分成训练集和测试集两部分
      val Array(trainingData, testData) = vecDF.randomSplit(Array(0.9, 0.1))
      testDFGlobal = testData
      trainDFGlobal=trainingData

      // 梯度提升树回归模型
      val gbt = new GBTRegressor()
        .setLabelCol(labelcol)
        .setFeaturesCol("indexedFeatures")
        .setMaxIter(10)

      // Chain indexer and GBT in a Pipeline.
      val pipeline_gbt = new Pipeline()
        .setStages(Array(featureIndexer, gbt))

      // 在训练集上训练梯度提升树回归模型
      val Model_gbt = pipeline_gbt.fit(trainingData)
      //在测试集上进行预测
      val predictions_gbt=Model_gbt.transform(testData)
      //计算模型在测试集上的平均相对误差
      val predictAndLabel_gbt=predictions_gbt.select(labelcol,"prediction").rdd.map(x=>(x(0).asInstanceOf[Double],x(1).asInstanceOf[Double]))
      val MAPE_gbt=predictAndLabel_gbt.map(value=>
      {
        math.abs(value._1-value._2)/value._1
      }).mean()
      //对全局模型变量和全局平均相对误差进行赋值
      tvModel=Model_gbt
      MAPE=MAPE_gbt


      //决策树回归模型
      val dt = new DecisionTreeRegressor()
        .setLabelCol(labelcol)
        .setFeaturesCol("indexedFeatures")

      val pipeline_dt = new Pipeline()
        .setStages(Array(featureIndexer,dt))
      // 在训练集上训练决策树回归模型
      val Model_dt=pipeline_dt.fit(trainingData)
      //计算模型在测试集上的平均相对误差
      val predictions_dt=Model_dt.transform(testData)
      //计算模型在测试集上的平均相对误差
      val predictAndLabel_dt=predictions_dt.select(labelcol,"prediction").rdd.map(x=>(x(0).asInstanceOf[Double],x(1).asInstanceOf[Double]))
      val MAPE_dt=predictAndLabel_dt.map(value=>
      {
        math.abs(value._1-value._2)/value._1
      }).mean()
      //比较决策树算法测试集的平均相对误差与全局平均相对误差的大小，如果决策树算法的平均相对误差比全局小，则对全局平均相对误差用决策树平均相对误差重新进行赋值，否则不变。
      if(MAPE_dt<MAPE){
        tvModel=Model_dt
        MAPE=MAPE_dt
      }
      //随机森林回归算法
      val rf = new RandomForestRegressor()
        .setLabelCol(labelcol)
        .setFeaturesCol("features")
      val pipeline_rf = new Pipeline()
        .setStages(Array(featureIndexer,rf))
      // 在训练集上训练随机森林回归模型
      val Model_rf = pipeline_rf.fit(trainingData)
      //计算模型在测试集上的平均相对误差
      val predictions_rf=Model_rf.transform(testData)
      val predictAndLabel_rf=predictions_rf.select(labelcol,"prediction").rdd.map(x=>(x(0).asInstanceOf[Double],x(1).asInstanceOf[Double]))
      //比较随机森林算法测试集的平均相对误差与全局平均相对误差的大小，如果随机森林算法的平均相对误差比全局小，则对全局平均相对误差用随机森林平均相对误差重新进行赋值，否则不变。
      val MAPE_rf=predictAndLabel_rf.map(value=>
      {
        math.abs(value._1-value._2)/value._1
      }).mean()
      if(MAPE_rf<MAPE){
        tvModel=Model_rf
        MAPE=MAPE_rf
      }
      0
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        -1
    } }





  override def saveModel(modelFilePath: String): Integer= {
    //对模型进行保存
    tvModel.write.overwrite().save(modelFilePath)
    val bos = new FileOutputStream(modelFilePath + "ci");
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(inputColIndex)
    oos.flush()
    oos.close()
    bos.flush()
    bos.close()
    0
  }
  override def loadModel(modelFilePath: String): Integer ={
    //对模型进行加载
    tvModel=PipelineModel.load(modelFilePath)
    val bis = new FileInputStream(modelFilePath + "ci")
    val ois = new ObjectInputStream(bis)
    inputColIndex = ois.readObject.asInstanceOf[Array[Int]]
    ois.close()
    bis.close()
    0
  }

  override def predict(inputData: Array[T with Object]):Array[T with Object] = {
    try{
      //把数据变成DataFrame形式
      println("just in predict")
      val inputRows = inputData.asInstanceOf[Array[Row]]
      val schema = inputRows(0).schema
      schema.foreach(println)
      val rdd = spark.sparkContext.makeRDD(inputRows)
      var df_org = spark.createDataFrame(rdd, schema)
      df_org.show()
      //找出自变量所在列的列名
      var inputcol_new = new ArrayBuffer[String](inputColIndex.length)
      for (i <- 0 until inputColIndex.length)
        inputcol_new += df_org.columns(inputColIndex(i))
      val inputcol = inputcol_new.toArray[String]
      //把数据类型变成Double类型
      for (column<-df_org.columns){
        df_org = df_org.withColumn(column,col(column).cast(DoubleType))
      }
      df_org.show(20)
      println("this is in predict")
      //提取特征指标数据,将多列数据转化为单列的向量列
      val vecDF: DataFrame = new VectorAssembler().setInputCols(inputcol).setOutputCol("features").transform(df_org)
      //进行预测
      val predictions=tvModel.transform(vecDF)
      //提取预测所在的列
      val predictionValue=predictions.select("prediction")
      predictionValue.printSchema()
      //把数据由DataFrame形式变成Array[T with Object]类型
      val newArray = predictionValue.collect()
      newArray.take(10).foreach(println)
      newArray.asInstanceOf[Array[T with Object]]
    }catch {
      case e:Exception=>
        e.printStackTrace()
        null
    }


  }


  override def getRMSE(inputData: Array[T with Object], labelColIndex1: Integer):java.lang.Double = {
    try {
      //目标变量列名
      val label=testDFGlobal.columns(labelColIndex(0))
      //对测试集上的数据进行预测
      val predictions=tvModel.transform(testDFGlobal)
      val predictAndLabel=predictions.select(label,"prediction").rdd.map(x=>(x(0).asInstanceOf[Double],x(1).asInstanceOf[Double]))
      //计算相对误差
      val MAPE=predictAndLabel.map(value=>
      {
        math.abs(value._1-value._2)/value._1
      }).mean()
      MAPE
    }catch {
      case e:Exception=>
        e.printStackTrace()
        1.0
    }
  }
}
