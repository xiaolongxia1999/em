package com.bonc.models

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import breeze.plot.{Figure, plot}
import net.sf.json.JSONObject
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.regression._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions.{max, min, _}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import com.bonc.interfaceRaw._
import com.bonc.utils.FAutils
/**
  * Created by patrick on 2018/4/12.
  * 作者：齐鹏飞
  */
class FactorAnalysis[T] extends IModel[T with Object] with Serializable{  //????????/
  private[this] val spark = SparkSession.builder().getOrCreate()
  var m_model:PipelineModel = null
  var m_modelParam = null
  var labelCol:String = _       //目标变量
  var observedCol = Array.empty[String]   //工况条件
  var testDataType = 2    //测试数据类型
  var selfClassify = 0    //是否自定义工况
  var workcond_tolnum = 0   //划分工况是否保留的阈值，需用户给定
  var bias = Array.emptyDoubleArray          //给定各工况的划分依据
  var testData = spark.emptyDataFrame               //测试数据
  var testDataArray = ArrayBuffer[DataFrame]()      //对应模型的测试数据集合数组
  var modelArray=ArrayBuffer[PipelineModel]()       //模型训练后得到的模型集合数组
  var model_result = ArrayBuffer[(PipelineModel,DataFrame)]()//训练完成后的模型和对应测试集的数组
  var centroids=Array.empty[Vector]                 //中心点集合
  var centroids_rows = ArrayBuffer[Int]()           //中心点包含的数据个数
  var selfFeaturing=1  //是否进行特征帅选（利用梯度提升树）
  var Gdbt_filteredCols = ArrayBuffer[Array[(String,Double)]]()
  var centroid_feature_import =Array[(Vector,Array[(String,Double)])]()
  var contrib_rate = 0.8
  var compute_model=1   //自定义偏差划分工况是，bias参数的含义，1-bias为对应的工况偏差值 0-bias为对应的工况分组数


  override def setParam(paramStr: String): Integer = {
    try{
      //读取参数
      val paramJson = JSONObject.fromObject(paramStr)    //读取JSON对象
      observedCol = paramJson.getJSONArray("observedCol").toArray.map(_.toString)
      observedCol.foreach(println)
      selfClassify = paramJson.getInt("selfClassify")
//      println(selfClassify)
      bias = paramJson.getJSONArray("bias").toArray.map(_.toString.toDouble)
//      println(bias)
      compute_model = paramJson.getInt("compute_model")
//      println(compute_model)
      selfFeaturing = paramJson.getInt("selfFeaturing")
//      println(selfFeaturing)
      contrib_rate = paramJson.getDouble("contrib_rate")
//      println(contrib_rate)
      workcond_tolnum = paramJson.getInt("workcond_tolnum")
//      println(workcond_tolnum)
      labelCol = paramJson.getString("labelCol")

//      println("labelCol----------------------------------")
//      println(labelCol)

      println("setparam is ok!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      0
    }catch {
      case e:Exception=>
        e.printStackTrace()
        -1
    }
  }

  override def train(inputData: Array[T with Object], inputColIndex: Array[Integer], labelColIndex:Array[Integer]): Integer = {
    try{
      //读取数据并进行分别训练
      val inputRows = inputData.asInstanceOf[Array[Row]]//设置输入数据的格式为ARRAY[ROW]行数组？
      val schema = inputRows(0).schema//提取数据结构
      val rdd = spark.sparkContext.makeRDD(inputRows)//将行数组转化成RDD
      val df = spark.createDataFrame(rdd,schema).repartition(1)  //根据RDD和结构构建dataframe，并设置分区数为1
      df.cache()  //数据缓存
      labelCol = df.columns(labelColIndex(0))    //目标变量的列名
      println("labelCol++： "+labelCol)
      var labelCol_1 =labelCol
      //println(df.count())
      var workcond_m = Array.empty[Vector]
      //判断是否为自定义工况
      val df_observed = df.select(observedCol.map(x=>col(x)):_*)
      df_observed.cache()
      if(selfClassify==1){
        //自定义工况，按偏差对工况进行固定切分，保留每个临界点作为中心点。现阶段只支持一个
        workcond_m = WorkCondition(df_observed,observedCol,bias,compute_model)
        workcond_m
      }else if(selfClassify==0){
        //利用聚类方法进行划分，保留中心点。
       // val restCol = df.columns.diff(observedCol)  //?.diff()   observedCol边界
        workcond_m = classifyObservation(df_observed)//?.drop()      //调用自动聚类的方法
      }

      // 根据工况的最小限制来对工况进行二次筛选，若限制阈值为0,则不进行筛选（但当
      centroids = Second_divi_work_condition(df_observed, observedCol, workcond_m, workcond_tolnum)

      centroids.foreach(println)
      var dist = Array.emptyDoubleArray  //空的double数组


      //根据中心点划分不同数据集
      for (i<-centroids.indices){

        //////////////////////////////////////////////////////////////////////////
        val df_new=df.filter{row=>
          val observed = Vectors.dense(observedCol.map(col=>row.getAs[Double](col)))   //？？？？？？

          dist =centroids.map(v=>Vectors.sqdist(v,observed))
          dist.indexOf(dist.min)==i
        }
        df_new.cache()

        val restCol = df.columns.diff(observedCol)


        //df_new.show()
        println(s"rows under condition${i} : "+df_new.count())
        //记录每一类下的数据个数以及及模型
        centroids_rows.append(df_new.count().toInt)  //记录每类有的样本数
        val selectedcol = df_new.columns.diff(observedCol).map(x=>col(x))

        //println("===========================================")
        val gbdt_selected_feature = FAutils.GBDTFS(df_new.select(selectedcol:_*),labelCol_1,contrib_rate,selfFeaturing)//特征筛选_GBDTFS进行指标筛选
        val filteredCols = gbdt_selected_feature.map(x=>x._1)
        val filteredCols1 = filteredCols.union(Array(labelCol))

       // gbdt_selected_feature.foreach(println)
       // gbdt_selected_feature.foreach(println)
        Gdbt_filteredCols.append(gbdt_selected_feature)

        println("最终特征数量: "+filteredCols.length)
       // df_new.select(filteredCols1.map(x=>col(x)):_*).show()
        modelArray.append(trainModel(df_new.select(filteredCols1.map(x=>col(x)):_*) ))       //按类进行模型训练，并将结果保存早modelArray中
        println("----------------------------------------------")
      }
      centroid_feature_import = centroids.zip(Gdbt_filteredCols)
      for (i <- centroid_feature_import.indices){

        println(s"工况：${centroid_feature_import(i)._1}下，特征重要度：")
        centroid_feature_import(i)._2.foreach(println)

        //Gdbt_filteredCols(i).foreach(println)

        //kkk(i)
      }

      println("trian is ok!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      0
    }catch {
      case e:Exception=>
        e.printStackTrace()
        -1
    }

  }

  override def saveModel(modelFilePath: String): Integer = {
    try{
      //保存模型以及对应的测试数据集
      for(i<-modelArray.indices){
        modelArray(i).write.overwrite.save(modelFilePath+s"${i}")
        testDataArray(i).coalesce(1).write.option("header",true).mode(SaveMode.Overwrite).csv(modelFilePath+"testData"+s"${i}")
      }
      var bos = new FileOutputStream(modelFilePath + "ci0")
//      var bos = new FileOutputStream("/home/biop/data/wyz/workingcond/save/" + "ci0")
      var oos = new ObjectOutputStream(bos)
      //保存目标变量名称
      oos.writeObject(labelCol)
      oos.flush()
      oos.close()
      bos.flush()
      bos.close()

      bos = new FileOutputStream(modelFilePath + "ci1")
//      bos = new FileOutputStream("/home/biop/data/wyz/workingcond/save/" + "ci1")
      oos = new ObjectOutputStream(bos)
      oos.writeObject(observedCol)
      oos.flush()
      oos.close()
      bos.flush()
      bos.close()

      bos = new FileOutputStream(modelFilePath + "ci2")
//      bos = new FileOutputStream("/home/biop/data/wyz/workingcond/save/" + "ci2")
      oos = new ObjectOutputStream(bos)
      //保存划分中心点和附属数据个数
      oos.writeObject(Array(centroids,centroids_rows))
      oos.flush()
      oos.close()
      bos.flush()
      bos.close()

      bos = new FileOutputStream(modelFilePath + "ci3")
      //      bos = new FileOutputStream("/home/biop/data/wyz/workingcond/save/" + "ci3")
      oos = new ObjectOutputStream(bos)
      oos.writeObject(centroid_feature_import)
      oos.flush()
      oos.close()
      bos.flush()
      bos.close()

      println("savemodel is ok!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      0
    }catch {
      case e:Exception=>
        e.printStackTrace()
        -1
    }

  }

  override def loadModel(modelFilePath: String): Integer = {
    try{
      var bis = new FileInputStream(modelFilePath + "ci0")  //？？？？？？？？
//      var bis = new FileInputStream("/home/biop/data/wyz/workingcond/save/"+ "ci0")
      var ois = new ObjectInputStream(bis)
      //读取目标变量名称
      labelCol=ois.readObject.asInstanceOf[String]
      ois.close()
      bis.close()

      bis = new FileInputStream(modelFilePath + "ci1")     //？？？？？？？？？？
//      bis = new FileInputStream("/home/biop/data/wyz/workingcond/save/"+ "ci1")
      ois = new ObjectInputStream(bis)
      observedCol = ois.readObject.asInstanceOf[Array[String]]
      ois.close()
      bis.close()

      bis = new FileInputStream(modelFilePath + "ci2")     //？？？？？？？？？？
//      bis = new FileInputStream("/home/biop/data/wyz/workingcond/save/"+ "ci2")
      ois = new ObjectInputStream(bis)
      //读取划分中心点和附属个数
      val centroidsArray =ois.readObject.asInstanceOf[Array[AnyRef]]
      centroids=centroidsArray(0).asInstanceOf[Array[Vector]]
      centroids_rows=centroidsArray(1).asInstanceOf[ArrayBuffer[Int]]
      ois.close()
      bis.close()


      bis = new FileInputStream(modelFilePath + "ci3")     //？？？？？？？？？？
      //      bis = new FileInputStream("/home/biop/data/wyz/workingcond/save/"+ "ci3")
      ois = new ObjectInputStream(bis)
      //读取划分中心点和附属个数
      centroid_feature_import =ois.readObject.asInstanceOf[Array[(Vector,Array[(String,Double)])]]


      ois.close()
      bis.close()
      //centroid_feature_import.map(x=>s"工况:${x._1},选取特征及重要度${x._2}").foreach(println)
    val  model = 0
      if (model == 1) {
        //读取模型信息和测试集
        testData = spark.read.option("header", true).option("inferSchema", true).csv(modelFilePath + "testData" + s"${testDataType}") //按需求读取对应的数据集

        // testDataArray.clear()  //清空
        modelArray.clear() //清空
        for (i <- centroids.indices) {
          modelArray.append(PipelineModel.load(modelFilePath + s"${i}")) //读取保存的模型
          //testDataArray.append(spark.read.option("header",true).option("inferSchema",true).csv(modelFilePath+"testData"+s"${i}"))
        }
        m_model = PipelineModel.load(modelFilePath + s"${testDataType}") //读取与测试工况一致的模型

        if (m_model.stages.last.isInstanceOf[LinearRegressionModel]) {

          val lrModel = m_model.stages.last.asInstanceOf[LinearRegressionModel]
          val predictions = m_model.transform(testData)
          //输出权重系数
          val coeff = lrModel.coefficients.toArray.zip(predictions.columns.filter(col => col != labelCol & col != "prediction" & col != "featureVector")).sortBy(x => math.abs(x._1)).reverse
          println("Here are the coefficients:")
          coeff.foreach(println)
        } else {
          val lrModel = m_model.stages.last.asInstanceOf[GBTRegressionModel]
          val predictions = m_model.transform(testData)
          val sortedFeatureImportanceArray = lrModel.featureImportances.toArray.zip(predictions.columns.filter(col => col != labelCol & col != "prediction" & col != "featureVector")).sorted.reverse
          println("Here are the Importance of Feature:")
          sortedFeatureImportanceArray.foreach(println)
        }
      }else{
        modelArray.clear() //清空
        for (i <- centroids.indices) {
          modelArray.append(PipelineModel.load(modelFilePath + s"${i}")) //读取保存的模型
        }
        testDataArray.clear() //清空
        for (i <- centroids.indices) {
          testDataArray.append(spark.read.option("header", true).option("inferSchema", true).csv(modelFilePath + "testData" + s"${i}")) //读取保存的模型
        }
        model_result = modelArray.zip(testDataArray)

        for(i <- model_result.indices){
          m_model = model_result(i)._1
          testData = model_result(i)._2
          if (m_model.stages.last.isInstanceOf[LinearRegressionModel]) {

            val lrModel = m_model.stages.last.asInstanceOf[LinearRegressionModel]
            val predictions = m_model.transform(testData)
            //输出权重系数
            var coeff = lrModel.coefficients.toArray.zip(predictions.columns.filter(col => col != labelCol & col != "prediction" & col != "featureVector")).sortBy(x => math.abs(x._1)).reverse
            println(s"For working conditions:${centroids(i)}, Here are the coefficients:")
            coeff.foreach(println)
          } else {
            val lrModel = m_model.stages.last.asInstanceOf[GBTRegressionModel]
            val predictions = m_model.transform(testData)
            var sortedFeatureImportanceArray = lrModel.featureImportances.toArray.zip(predictions.columns.filter(col => col != labelCol & col != "prediction" & col != "featureVector")).sorted.reverse
            println(s"For working conditions:${centroids(i)},Here are the Importance of Feature:")
            sortedFeatureImportanceArray.foreach(println)
          }
        }
      }
      println("loadmodel is ok!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      0
    }catch{
      case e:Exception=>
        e.printStackTrace()
        -1
    }
  }

  override def predict(inputData: Array[T with Object]): Array[T with Object] = {
    try{
          val inputRows = inputData.asInstanceOf[Array[Row]]
          val schema = inputRows(0).schema
          val rdd = spark.sparkContext.makeRDD(inputRows)
          var df = spark.createDataFrame(rdd,schema).repartition(1)
          df.cache()
          var dist = Array.emptyDoubleArray  //空的double数组
          var ab = new ArrayBuffer[Double]()
          //首先判断工况情况，在根据工况进行调取对应的工况模型，但是
          val restCol = df.columns.diff(observedCol)

          val observed = df.drop(restCol:_*).rdd.map(x=>
          {for(j <- 0 until observedCol.length){
                ab.append(x.getDouble(j))
              }
              ab.toArray
            }).collect().map(Vectors.dense(_))

          dist = centroids.map(v => Vectors.sqdist(v, observed(0)))
          var model_num = dist.indexOf(dist.min)

          println("model_num = "+model_num)
          println("工况="+centroids(model_num))
          val predictions = modelArray(model_num).transform(df)

         // predictions.select(labelCol,"prediction").take(1).map(x=>s"labelCol=${x.getDouble(0)};prediction=${x.getDouble(1)}").foreach(println)
          predictions.select("prediction").take(1).map(x=>s"prediction=${x.getDouble(0)}").foreach(println)
          //println("predict is ok!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      predictions.select("prediction").collect()
        .asInstanceOf[Array[T with Object]]
    }catch {
      case e:Exception=>
        e.printStackTrace()
        null
    }
  }

  override def getRMSE(inputData: Array[T with Object], labelColIndex: Integer): java.lang.Double = {
    try{

      //是否修改为计算所有工况的模型效果
      //根据测试数据进行模型评估
      val f = Figure()
      val rmse = new ArrayBuffer[Double]()
      for(i <- model_result.indices) {

        val predictions = model_result(i)._1.transform(model_result(i)._2) //预测
        val rmse1 = predictions.select(labelCol,"prediction").collect().map(x=>math.abs((x.getDouble(1)-x.getDouble(0))/x.getDouble(0))).sum/predictions.count()
        println(s"工况:${centroids(i)}的平均相对误差："+ rmse1)
        rmse.append(rmse1)

        val showResultsInFigure = 1
        val test_pred = model_result(i)._1.transform(model_result(i)._2).select("prediction").collect()
        if (showResultsInFigure == 1 & model_result(i)._2.count() > 0 & test_pred.length > 0) {

          val p = f.subplot(2,math.ceil(model_result.length.toDouble/2.0).toInt,i)
          val x = (0 until (model_result(i)._2.count().toInt)).toArray.map(_.toDouble)
          val y_pred = test_pred.map(row => row.getDouble(0))
          val y_label = model_result(i)._2.select(labelCol).collect().map(row => row.getDouble(0))
          p += plot(x, y_pred, name = "predictions", colorcode = "r")
          p += plot(x, y_label, name = labelCol, colorcode = "b")
          p.legend = true
          p.title = s"工况${centroids(i)}下模型效果"
          p.title.charAt(1)
          p.yaxis.setAutoRange(true)
          p.yaxis.setAutoRangeIncludesZero(true)
//
        }

        //===============================================================================
        //评价
//        val evaluator = new RegressionEvaluator()
//          .setLabelCol(labelCol)
//          .setPredictionCol("prediction")
//          .setMetricName("mae")
//        //r2：拟合优度  rmse:均方根误差  mae:平均绝对误差
//        val rmse = evaluator.evaluate(predictions)
//
//
//
//        println(s"工况:${centroids(i)}的平均相对误差：" + rmse / predictions.select(labelCol).groupBy().mean().first().getDouble(0))
      }
      val evaluator_result = centroids.map(x=>x.toString).zip(rmse)
      //evaluator_result.foreach(println)
      println("getrmse is ok!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      0
    }catch {
      case e:Exception=>
        e.printStackTrace()
        -1
    }
  0.0
  }

  //keams聚类划分工况
  def classifyObservation(df:DataFrame): Array[Vector] ={
    df.cache()
    val upLimit = math.sqrt(df.count().toDouble/2).toInt    //设定聚类个数的最高限
    println("upLimit "+upLimit)
    //(20 to upLimit by 20).map(k => (k, clusteringScore2(df, k))).foreach(println)
    var clusteringCost = 0.0    //计算记录到中心点的距离和
    //var finalCost = 100.0       //中间变量
    var finalCost = clusteringScore2(df,2)
    var stopCon = 100.0         //停止条件，如果下降率达到某一值
    var bestK = 100             //记录性价比最高的K值
    for(k<-3 to upLimit by 2){
      if(stopCon>0.06){
        clusteringCost=clusteringScore2(df,k)
        stopCon=(finalCost-clusteringCost)/finalCost
        finalCost=clusteringCost
        bestK=k
        println(s"k:${k},cost:${clusteringCost}")
      }
    }

    val va = new VectorAssembler()  //向量汇编
      .setInputCols(df.columns)
      .setOutputCol("featureVector")

    val initMode = "k-means||"  //adopt the k-means ++ algorithm to calculate the initial clustering centers
    val km = new KMeans()
      .setSeed(Random.nextLong() ) //随机种子？)
      .setInitMode(initMode)      //
      .setFeaturesCol("featureVector")  //特征列设置
      .setPredictionCol("cluster")      //预测列设置
      .setK(bestK)                       //分类数设置
      .setTol(1.0e-5)
      .setMaxIter(40)                   //迭代次数设置


    val pipeLine = new Pipeline().setStages(Array(va,km))
    val pipeLineModel = pipeLine.fit(df)
    val kmModel = pipeLineModel.stages.last.asInstanceOf[KMeansModel]   //这里.last意义？？？？？？

    val centroids = kmModel.clusterCenters    //找出所有的聚类中心点

    centroids
  }

  //聚类效果
  def clusteringScore2(data: DataFrame, k: Int): Double = {
    //用来对每个k值进行聚类计算的模块
    val assembler = new VectorAssembler().
      setInputCols(data.columns).
      setOutputCol("featureVector")

    val kmeans = new KMeans().
      setSeed(Random.nextLong()).
      setK(k).
      setInitMode("k-means||").
      setPredictionCol("cluster").
      setFeaturesCol("featureVector").
      setMaxIter(40).
      setTol(1.0e-5)
    val pipeline = new Pipeline().setStages(Array(assembler, kmeans))
    val pipelineModel = pipeline.fit(data)
    val kmeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]  ///？？？？？？？
    kmeansModel.computeCost(pipelineModel.transform(data)) / data.count()
  }

  //预测模型拟合函数
  def trainModel(inputDF:DataFrame):PipelineModel={
    //对不同工况下的模型进行训练
    //进行特征选择
    var df = inputDF


    //进行测试集和训练集的划分
    val Array(trainData,testData) = df.randomSplit(Array(0.8,0.2),Random.nextLong())
    trainData.cache()
    testData.cache()

    println("工况数据样本数："+df.count())
    println("训练数据样本数："+trainData.count())
    println("测试数据样本数："+testData.count())
    //记录测试集集合
    testDataArray.append(testData)
    //进行建模训练
    val va = new VectorAssembler()
      .setInputCols(trainData.columns.filter(_!=labelCol))
      .setOutputCol("featureVector")

    val sa = new StandardScaler()
      .setInputCol("featureVector")
      .setOutputCol("scaledVector")

    val lr = new LinearRegression()
      .setFeaturesCol("scaledVector")
      .setLabelCol(labelCol)
      .setPredictionCol("prediction")
      .setRegParam(0.4)
    val pl = new Pipeline().setStages(Array(va,sa,lr))
    val plModel = pl.fit(trainData)

    val predictions_1 =plModel.transform(testData)

    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol("prediction")
      .setMetricName("mae")
    //r2：拟合优度  rmse:均方根误差  mae:平均绝对误差
    val rmse_1 = evaluator.evaluate(predictions_1) / predictions_1.select(labelCol).groupBy().mean().first().getDouble(0)

    var gbtr = new GBTRegressor()
      .setFeaturesCol("scaledVector")
      .setLabelCol(labelCol)
      .setPredictionCol("prediction")
      .setMinInfoGain(0.0)
      .setMaxDepth(3)
      .setMaxIter(10)
      .setMaxBins(20)

    val p2 = new Pipeline().setStages(Array(va,sa,gbtr))

    val p2Model = p2.fit(trainData)

    val predictions_2 =p2Model.transform(testData)
    val rmse_2 = evaluator.evaluate(predictions_2) / predictions_2.select(labelCol).groupBy().mean().first().getDouble(0)
    //输出模型
    if(rmse_1<=rmse_2){
      plModel
    }else{
      p2Model
    }

  }

  def WorkCondition(df:DataFrame,bj:Array[String],basis:Array[Double],compute_model:Int=1):Array[Vector]={

    val boundaryCol = bj.zip(basis)
    var duo_array = new Array[Array[String]](boundaryCol.length)
    for(i<-boundaryCol.indices){
      var minValue = df.agg(min(boundaryCol(i)._1)).head().getDouble(0)-1e-8
      var maxValue = df.agg(max(boundaryCol(i)._1)).head().getDouble(0)+1e-8
      var basis_1 = 0.0
      if (compute_model == 1){
        basis_1 = boundaryCol(i)._2/2.0
      }else{
        basis_1 = (maxValue-minValue)/boundaryCol(i)._2/2.0
      }

      var centro = 0.0
      val cl = ArrayBuffer[String]()  //空的可变数组
      while(minValue<maxValue){
        centro = minValue+basis_1
        cl.append(centro.toString)
        minValue+=basis_1*2.0
      }
      duo_array(i)=cl.toArray
    }

    duo_array.reduce((x,y)=> x.flatMap(e=>y.map(f=>e+","+f))).map(x=>x.split(",").map(_.toDouble)).map(Vectors.dense(_))
  }

  def Second_divi_work_condition(df:DataFrame,bj:Array[String],workcond_center:Array[Vector],workcond_num:Int): Array[Vector] ={
    var dist = Array.emptyDoubleArray  //空的double数组
    var wc_num =  new ArrayBuffer[Double]()

    for(i <- workcond_center.indices){
      wc_num.append(df.filter{row=>
        val observed = Vectors.dense(bj.map(col=>row.getAs[Double](col)))   //？？？？？？
        dist =workcond_center.map(v=>Vectors.sqdist(v,observed))
        dist.indexOf(dist.min)==i
      }.count())
    }

    workcond_center.zip(wc_num).filter(x => {x._2.toDouble>workcond_num}).map(x =>x._1)
  }

//  def GBDTFS(df:DataFrame,labelCol:String,contrib_rate1:Double,is_filter:Int):Array[(String,Double)]={
//    val inputCols=df.columns.filter(_!=labelCol)
//    val vectorAssembler = new VectorAssembler()
//      .setInputCols(inputCols)
//      .setOutputCol("featureVector")
//
//    val GBDTRegressor = new GBTRegressor()
//      .setFeaturesCol("featureVector")
//      .setLabelCol(labelCol)
//      .setPredictionCol("prediction")
//      .setMinInfoGain(0.0)
//      .setMaxDepth(3)
//      .setMaxIter(10)
//      .setMaxBins(20)
//
//    val pipeLine = new Pipeline().setStages(Array(vectorAssembler,GBDTRegressor))
//    val plModel = pipeLine.fit(df)
//    //val prediction = plModel.transform(df)
//    val GBDTRegressionModel = plModel.stages.last.asInstanceOf[GBTRegressionModel]
//    val sortedFeatureImportanceArray = GBDTRegressionModel.featureImportances.toArray.zip(inputCols).sorted.reverse
//    //val filteredCols1 = sortedFeatureImportanceArray.filter(_._1>threshold).map(x=>x._2)
//
//    //梯度提升指标筛选
//    if (is_filter == 1) {
//      var contribution_Rate = 0.0
//      var i = 1
//      while (contribution_Rate <= contrib_rate1) {
//        contribution_Rate = contribution_Rate + sortedFeatureImportanceArray.take(i).last._1
//        i += 1
//      }
//      //val filteredCols = sortedFeatureImportanceArray.take(i-1).map(x=>x._2)
//      sortedFeatureImportanceArray.take(i - 1).map(x => (x._2, x._1))
//      //val dropCols=inputCols.filterNot(x=>filteredCols.contains(x))
//      // val filteredDF = df.drop(dropCols:_*)
//      //return filteredDF
//    }else{
//      sortedFeatureImportanceArray.map(x=>(x._2,x._1))
//    }
//  }
}
