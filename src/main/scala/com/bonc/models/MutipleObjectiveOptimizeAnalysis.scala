package com.bonc.models

import java.lang

import com.bonc.interfaceRaw.IModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.{GeneralizedLinearRegression, GeneralizedLinearRegressionModel, LinearRegression}
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col

/**
  * Created by Administrator on 2018/8/22 0022.
  */
class MutipleObjectiveOptimizeAnalysis[T] extends IModel[T with Object] with Serializable {

  @transient
  val conf:SparkConf = new SparkConf().setAppName("Mo optimize analysis").set("spark.shuffle.consolidateFiles", "true") //.setJars(Array("sparkts-0.4.0-SNAPSHOT-jar-with-dependencies.jar"))
  @transient
  val sparkContext: SparkContext = SparkContext.getOrCreate(conf)
  @transient
  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

//  仿照spark ml设置参数的命令：org.apache.spark.ml.param包
//  实际使用参照文档pipeline里parameters一节
//  val fam: Param[String] = new Param(this, "fam", "a test", (value:String) => value.contains("f") )
//  def setFam(value: String): this.type = set(fam, value );setDefault(fam->"fgaussian")
//  def getFam(): String = $(fam)

  def train(inputData: Array[T with Object], inputColIndex: Array[Integer], labelColIndex: Array[Integer]): Integer = ???

  def predict(inputData: Array[T with Object]): Array[T with Object] = ???

  //注意：隐藏参数inputCols是指————一个初始的Dataframe(csv或传感器读取）,前输入多少列，只留下需要的列——设置此，是为了用户只需要修改json参数，不用重新勾选，或修改并上传csv
  //outputCol是指————在train和predict阶段，输出哪些列————这个用处？？？
  //inputColIndex和labelColIndex——————分别用于指定：特征向量（Array)和标签向量
  def setParam(params: String): Integer = ???

  def saveModel(modelFilePath: String): Integer = ???

  def loadModel(modelFilePath: String): Integer = ???

  def getRMSE(inputData: Array[T with Object], labelColIndex: Integer): lang.Double = ???

  def init(inputData: Array[T with Object]): DataFrame

  def preProcess(inputData: Array[T with Object], exprs: Array[String]): DataFrame = ???



  /**
    * 从原始输入数据，选取指定索引的列，提取待分析数据
    * @param inputData
    * @param inputColIndex 特征变量的索引
    * @return DataFrame
    */
  def extractData(inputData: Array[T with Object], inputColIndex: Array[Integer]): DataFrame = {
    val rawData = inputData.asInstanceOf[Array[Row]]
    val schema = rawData(0).schema
    val rawRDD = sparkContext.parallelize(rawData)
    var rawDataFrame = spark.createDataFrame(rawRDD, schema)
    val selectedFields = inputColIndex.map(index => schema.fieldNames(index))
    val selectedCols = selectedFields.map(colName => col(colName) )
    rawDataFrame = rawDataFrame.select(selectedCols:_*)
    rawDataFrame
  }
  //对“目标变量"与"决策变量"作非线性拟合，使用Generalized Linear Regression

  /**
    * 单目标变量与决策变量的广义线性拟合
    * @param training  原始输入数据
    * @param featuresCols 特征列
    * @param labelCol 标签列
    * @param params 参数设置
    * @return
    */
  def fitOneObjectiveVariable(training: DataFrame,
                              featuresCols: Array[Integer],
                              labelCol: Integer,
                              params: ParamMap):GeneralizedLinearRegressionModel = {
    val glr = new GeneralizedLinearRegression()
    glr.fit(training, params)
  }

  def getCoefficients(glrModel: GeneralizedLinearRegressionModel): Vector = glrModel.coefficients

  //拟合N个不同的目标函数，获取coefficients
  //对于每个目标函数的拟合，要使用所有不同的连接函数->指数分布组合，通过评估指标，选取最合适的一个拟合结果
  //spark ML目前支持的组合见：
  /**
    * :: Experimental ::
    *
    * Fit a Generalized Linear Model
    * (see <a href="https://en.wikipedia.org/wiki/Generalized_linear_model">
    * Generalized linear model (Wikipedia)</a>)
    * specified by giving a symbolic description of the linear
    * predictor (link function) and a description of the error distribution (family).
    * It supports "gaussian", "binomial", "poisson", "gamma" and "tweedie" as family.
    * Valid link functions for each family is listed below. The first link function of each family
    * is the default one.
    *  - "gaussian" : "identity", "log", "inverse"
    *  - "binomial" : "logit", "probit", "cloglog"
    *  - "poisson"  : "log", "identity", "sqrt"
    *  - "gamma"    : "inverse", "identity", "log"
    *  - "tweedie"  : power link function specified through "linkPower". The default link power in
    *  the tweedie family is 1 - variancePower.
    */
  def fitObjectiveVariables(features: Array[Tuple2[DataFrame, Integer]]): Array[Tuple2[DataFrame,Vector]]= ???


}


object MutipleObjectiveOptimizeAnalysis {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("MOEA")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //测试线性回归拟合

    val inputPath = "E:\\bonc\\工业第四期需求\\数据\\out\\selectedVariable0816.csv"
    // Load training data
    var training = spark.read.format("csv")
      .option("header", true)
      .option("infer_schema", true)
      .load(inputPath)

    for(column<-training.columns) {
      training = training.withColumn(column, col(column).cast(DoubleType))
    }

    val columns = training.columns
    val colSize = columns.length
    val inputCols = (1 to colSize -1).map(x => columns(x) ).toArray
    val labelCol = (columns(0))
    val assembler = new VectorAssembler()
      .setInputCols(inputCols)
      .setOutputCol("features")
    val transferedDF = assembler.transform(training)


    val lr = new LinearRegression()
       .setElasticNetParam(0.5)
        .setFeaturesCol("features")
        .setLabelCol(labelCol)
        .setMaxIter(100)
        .setRegParam(0.1)
        .setFitIntercept(true)
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(transferedDF)

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")


  }
}