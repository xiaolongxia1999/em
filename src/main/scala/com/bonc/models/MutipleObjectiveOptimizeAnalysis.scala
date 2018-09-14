package com.bonc.models

import java.lang

import com.bonc.interfaceRaw.IModel
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{Imputer, VectorAssembler}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.{GeneralizedLinearRegression, GeneralizedLinearRegressionModel, LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by Administrator on 2018/8/22 0022.
  */

/**  117 - 19
class MutipleObjectiveOptimizeAnalysis[T] extends IModel[T with Object] with Serializable {

//  @transient
//  val conf:SparkConf = new SparkConf().setAppName("Mo optimize analysis").set("spark.shuffle.consolidateFiles", "true") //.setJars(Array("sparkts-0.4.0-SNAPSHOT-jar-with-dependencies.jar"))
//  @transient
//  val sparkContext: SparkContext = SparkContext.getOrCreate(conf)
//  @transient
//  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

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


//
//  /**
//    * 从原始输入数据，选取指定索引的列，提取待分析数据
//    * @param inputData
//    * @param inputColIndex 特征变量的索引
//    * @return DataFrame
//    */
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
*/

//@warn 测试时版本已经替换为2.2.0
//@深刻教训，命名时，不要用“—”，“_”这种符号，严格按照字母命名，在spark sql里，容易出错，尤其是用expressions时
object MutipleObjectiveOptimizeAnalysis {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("MOEA")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //测试线性回归拟合

    val inputPath = "E:\\bonc\\工业第四期需求\\数据\\out\\selectedVariable0816.csv"
    // Load training datasets
    var inputData = spark.read.format("csv")
      .option("header", true)
      .option("infer_schema", true)
      .load(inputPath)

    for(column<-inputData.columns) {
      inputData = inputData.withColumn(column, col(column).cast(DoubleType))
    }

    //防止后面对数化报错，之前fill("0")
    //DataFrameNaFunctions的三个功能：填充、替换、删除
    //填充(fill)：当为空值时，将null值直接用某值替换， 是“替换”的特殊情形， 被替换值为空而已
    //替换(replace)：将“指定被替换值” 替换成 “替换值”
    //删除（drop)
    val na_array = Array(-9D,0D)
    val map_na = na_array.map(x=> (x->Double.NaN)).toMap

//    inputData = inputData.na.replace(inputData.columns, Map(-9D->Double.NaN, 0D->Double.NaN))
    inputData = inputData.na.replace(inputData.columns, map_na)

    val imputer = new Imputer()
      .setInputCols(inputData.columns)
      .setOutputCols(inputData.columns)

    val model = imputer.fit(inputData)
    inputData = model.transform(inputData)//.show()

    //类型转换
//    for(column<-inputData.columns) {
//      inputData = inputData.withColumn(column, col(column).cast(DoubleType))
//        .withColumn(column, udf_relu(col(column)))
//    }



    //历史原因，我将缺失数据替换成-9了， 这里可以指定一个Array，里面包含缺失值内容。对于df中所有元素，只要在Array中，则替换为Double.NAN
    //替换为Double.NAN之后，用ML中的Imputer处理缺失值（平均数或中位数替换）
    //“缺失值——异常值”处理逻辑：
    //如果缺失值是文本，则需要先转成Double(即文本若能转成Double则直接替换为Double.NAN, 否则先转成某个Double.然后替换把该Double替换为Double.NAN,再用Imputer处理
//    val func = (x:Double, array:Seq[Double]) => x match {case x if array.contains(x) => Double.NaN; case _ => x}  //跑不通
//    val func = (x:Double, array:_*) => x match {case x if array.contains(x) => Double.NaN; case _ => x}
//    val func = (x:Double, value:Double) => x match {case x if x.equals(value) => Double.NaN; case _ => x} //报错：栈溢出java.lang.StackOverflowError
//    val udf_na_replace = udf( (x:Double, na:Array[Double]) => x match {
//      case x if na.contains(x) => Double.NaN
//      case _ => x
//    })


//    val udf_na_replace = udf(func)
    //将小于0的数字转成2,其余不变——————否则，后面的参数均不合法
    val udf_relu = udf( (x:Double) => x match {
      case  x if x <= 0 => 1
      case _ => x
    })
    //缺失值处理：使用平均数代替，如果简单用0或1这种数字代替，回归时容易导致其贡献很大的误差（实际上这条数据应该是缺失的
    //因为：本例数据中，离散程度较小，可以用平均值替代
    //处理逻辑：平均数代替————————对于缺失比例较高（如阀值50%）,则过滤掉该缺失值
    //本文中，默认给的数据质量较好


//    val na_array_double = na_array.map(x=>x.toDouble)
    for(column<-inputData.columns) {
      inputData = inputData
//        .withColumn(column, udf_na_replace(col(column), lit("-9"), lit("0")))
        .withColumn(column, udf_relu(col(column)))
    }


    inputData.printSchema()
    inputData.show()

    //引用原inputData,在此基础上转换

    /* 137 ~ 178
    var df = inputData

    for(column<-inputData.columns) {
      df = df.withColumn(column, col(column).cast(DoubleType))
    }

    val columns = df.columns
    val colSize = columns.length
    val inputCols = (1 to colSize -1).map(x => columns(x) ).toArray
    val labelCol = (columns(0))
    val assembler = new VectorAssembler()
      .setInputCols(inputCols)
      .setOutputCol("features")
    val transfered = assembler.transform(df)


    val lr = new LinearRegression()
       .setElasticNetParam(0.5)
        .setFeaturesCol("features")
        .setLabelCol(labelCol)
        .setMaxIter(10)
        .setRegParam(0.1)
        .setFitIntercept(true)
      .setStandardization(false)
//      .setMaxIter(100)
//      .setRegParam(0.3)
//      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(transferedDF)

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the df set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")
*/




//    模式识别部分
//    目标1
    val ferturesGoalone = Array("CZ3-FC7002","CZ3-TI7001","CZ3-TI7003","CZ3-TI7005","CZ3-TI7007")
//    val label_expr_one = "$(CZ3-FC7007) / $(CZ3-FC7002) * 100"

    var goalExprOne = col("CZ3-FC7007") / col("CZ3-FC7002") * lit(100)

    val goalNameOne = "label1"
//    目标2
    val ferturesGoalTwo = Array("CZ3-TI7041","CZ3-TI7043","CZ3-PI7008","CZ3-PI7101")
    val goalExprTwo = col("CZ3-FIC7009") / col("CZ3-FC7002") * lit(100)
    val goalNameTwo = "gitwo"

//    目标3
    val ferturesGoalThree = Array("CZ3-FI7017","CZ3-FI7018","CZ3-FI7019","CZ3-FI7020")
    val goalExprThree = col("CZ3-FI7017") + col("CZ3-FI7018") + col("CZ3-FI7019") + col("CZ3-FI7020")
    val goalNameThree = "githree"

    //目标4
    val ferturesGoalFour = Array("CZ3-FC7002", "CZ3-TI7001", "CZ3-TI7003", "CZ3-TI7005", "CZ3-TI7007")
    val goalExprFour = col("CZ3-FI7006") * lit(0.0000899) / col("CZ3-FC7002") * lit(100)
    val goalNameFour = "gifour"


    //生成对应的目标列
    var df1 = inputData
      .withColumn(goalNameOne, goalExprOne)
      .withColumn(goalNameTwo, goalExprTwo)
      .withColumn(goalNameThree, goalExprThree)
      .withColumn(goalNameFour, goalExprFour)

//    df1.coalesce(1).write.option("header",true).mode(SaveMode.Overwrite).csv("E:\\bonc\\工业第四期需求\\数据\\out\\out")
      df1.coalesce(1).write.option("header",true).mode(SaveMode.Overwrite).csv("E:\\bonc\\工业第四期需求\\数据\\out\\out1")

    //向量转换
    val assemblerOne = new VectorAssembler()
      .setInputCols(ferturesGoalone)
      .setOutputCol("featuresColOne")

    val assemblerTwo = new VectorAssembler()
      .setInputCols(ferturesGoalTwo)
      .setOutputCol("featuresColTwo")

    val assemblerThree = new VectorAssembler()
      .setInputCols(ferturesGoalThree)
      .setOutputCol("featuresColThree")

    val assemblerFour = new VectorAssembler()
      .setInputCols(ferturesGoalFour)
      .setOutputCol("featuresColFour")

//    df1 = assembler_one.transform(df1)
//    df1 = assembler_two.transform(df1)
//    df1 = assembler_three.transform(df1)
//    df1 = assembler_four.transform(df1)

    val pipelineOne = new Pipeline()
      .setStages(Array(assemblerOne, assemblerTwo, assemblerThree, assemblerFour))
    val pipelineModelOne = pipelineOne.fit(df1)
    df1 = pipelineModelOne.transform(df1)


    //    对数化——提高拟合程度————结果：效果很差
    val  udf_log = udf( (x:Double) => math.log(x))
    println("p0")
    val udf_verctor_log = udf( (x:Vector) => Vectors.dense(x.toDense.values.map(x=>math.log(x))) )
    //新增对数化后向量
    println("p1")
    df1 = df1.withColumn("log_featuresColOne", udf_verctor_log(col("featuresColOne")))
//    对数化原label1标签列
      .withColumn("label",udf_log(col("label1")))
    df1.show()

    println("p2")

    df1.show()
//    df1.coalesce(1).write.mode(SaveMode.Overwrite).csv("F:\\3")
    println("VectorAssembler is OK for pipeline--------------------------------------------------")
    //VectorAssembler虽没有fit，也可以使用PipeLine，可能是直接transform，不用fit

    //注意这里，貌似setLabelCol不能写成传入其他字符串，只能传"label"？ setFeaturesCol则可以
    val lr1 = new LinearRegression()
      .setElasticNetParam(0.0)
      .setFeaturesCol("featuresColOne")
      .setStandardization(true)
      .setFitIntercept(true)
      //拟合对数化后特征向量
//      .setFeaturesCol("log_featuresColOne")
//      .setgoalCol(goalNameOne)
      .setLabelCol("label1")
//      .setMaxIter(10)
      //正则化参数调整得越大， 系数w会很小， 导致欠拟合问题，所以需要找到合适的regParam.  先设置不同的量级上测试，找出最优（如0,10,100,1000）,再在对应量级上以之为中心搜索附近的范围
//      .setRegParam(0.1)
      .setFitIntercept(true)
//    单独用LinearRegressionModel测试
//    val lrModel = lr1.fit(df1)
//    val trainingSummary = lrModel.summary
//    println(s"numIterations: ${trainingSummary.totalIterations}")
//    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
//    trainingSummary.residuals.show()
//    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
//    println(s"r2: ${trainingSummary.r2}")
//    println(s"labelCol: ${trainingSummary.labelCol}")
//    println(s"featuresCol: ${trainingSummary.featuresCol}")
//    println(s"coefficients: ${lrModel.coefficients}")






    //第2个管道，只含LinearRegression过程
    val pipelineTwo = new Pipeline()
      .setStages(Array(lr1))

//    设置参数网格
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr1.fitIntercept, Array(true, false))
//      .addGrid(lr1.elasticNetParam, (1 to 9).map(x => x.toDouble/10)   )
//      此处貌似是5.0最好
//      .addGrid(lr1.regParam, Array(0.0, 0.5, 1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0))
      .addGrid(lr1.regParam, (0 to 99).map(x => 0.1 * x))
      .build()

//    val cv = new CrossValidator()
//      .setEstimator(pipeline)
//      .setEvaluator(new BinaryClassificationEvaluator)
//      .setEstimatorParamMaps(paramGrid)
//      .setNumFolds(2)

    val cv = new CrossValidator()
      .setEstimator(pipelineTwo)
      .setEvaluator(new RegressionEvaluator()
        //使用R^2作为评估器，不用默认的rmse
//        .setMetricName("r2"))
      .setMetricName("rmse"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    df1.printSchema()
    df1.show()

    val cvModel = cv.fit(df1)
//    val lrrr = lr1.fit(df1)

    cvModel.params.foreach(println)
    cvModel.explainParams()

    println("bm------------------------------------------")
    cvModel.bestModel.asInstanceOf[PipelineModel]
    println("")
    val pipeline33 = cvModel.bestModel.parent.asInstanceOf[Pipeline]

    //从pipeline中取出具体模型来
    val lrModel = cvModel.bestModel.asInstanceOf[PipelineModel].stages(0).asInstanceOf[LinearRegressionModel]

    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")
    println(s"labelCol: ${trainingSummary.labelCol}")
    println(s"featuresCol: ${trainingSummary.featuresCol}")
    println(s"coefficients: ${lrModel.coefficients}")


//    结论：
//    1.不适合对数模型，直接用原始特征作线性回归即可，r**2达到r2: 0.9996640071018034，对应参数如下：
//    regParam:9.9
//    elasticNet:0.0——>即只用L2不用L1，因这5个都是强相关参数
//    得到的参数
//    coefficients: [0.36428558110765863,0.02336489431134147,0.009196162632381348,0.009231917015050864,0.021842020326400736]

//    缺失值替换后结果，参数仍如上
//    RMSE: 1.294687377616038
//    r2: 0.9998149683505981
//    labelCol: label1
//    featuresCol: featuresColOne
//    coefficients: [0.13860404371733873,0.026846622691319245,0.0392918887756506,0.06027400054919808,0.011630008023741667]


//    val _result = cvModel.transform(1)

//    _result.show()

  }
}