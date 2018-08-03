package com.bonc.models

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import java.lang.Long
import java.sql.Date
import java.text.SimpleDateFormat

import breeze.numerics.ceil
import com.bonc.interfaceRaw.IModel
import net.sf.json.JSONObject
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random//java和scala集合转化，但用之后汇报其他错，未解决
/**
  * Created by patrick on 2018/4/13.
  * 作者：
  */
class AbnormalDetection[T] extends IModel[T with Object] with Serializable{
  private[this] val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._
  var m_model:PipelineModel = null
  var threshold_global = 0.0

  var timeWindow = 0      //提前预警的时间窗口
  var threshold_type = 0  //确定阈值方式1：百分比；0：均值+3*std
  var ratio =0.0          //按照确定距离阈值1方式时的比例值
  var train_anomalies:Array[Row] = null
  var df_rules:Array[Row] = null
  var sing_warn_t : Array[(String,Array[Double])] = null
  var param_rules : Array[(String,Array[Double])] = null
  var train_sing_anomalies:Array[Row] = null

  var km_start_k = 5  //循环确定km的k的初始值
  var km_by = 2      //确定bestk值时k步长
  var km_tol = 0.0    //循环确定bestk时终止条件阈值


  //==传递参数
  override def setParam(paramStr: String): Integer = {
    try{
      //val param = spark.read.json(paramStr).toJSON.select("value").collect().head(0).toString  //读取json文件
      //val ps = Source.fromFile(param).mkString

      val paramJson = JSONObject.fromObject(paramStr)
      ratio =paramJson.getDouble("ratio")
      timeWindow = paramJson.getInt("timeWindow")
      threshold_type = paramJson.getInt("threshold_type")
      //读取json形式的超限规则

      val alarmRules = paramJson.getJSONArray("alarmRules").iterator()
      var arrString = new ArrayBuffer[String]()
      while(alarmRules.hasNext){
        arrString.append(alarmRules.next().toString)
      }

      //===============================test==================================== @
      val rdd1 = spark.sparkContext.parallelize(arrString)
      var df_json  =  spark.read.format("json").option("header",true).json(rdd1)
      df_json = df_json.withColumn("high",col("high").cast(DoubleType)).withColumn("low",col("low").cast(DoubleType))


      for(colName2<-df_json.columns if colName2!="sensorID") {
        df_json = df_json.withColumn(colName2+"_1", when(col(colName2).isNull, 0)
          .when(col(colName2).isNotNull,1))
      }
     df_json = df_json.select($"sensorID",$"high",$"low",($"high_1"*2+$"low_1" as "type")).na.fill(Map("high"->1000000000,"low"->(-1000000000)))
      //单指标报警
      param_rules = df_json.select("sensorID","high","low").rdd.collect().drop(0).map(x=>(x.getString(0),Array(x.getDouble(1),x.getDouble(2))))

      df_rules = df_json.collect()




      //========================================================================@
      println("======setparams is ok")
      0

    }catch {
      case e:Exception=>
        e.printStackTrace()
        -1
    }
  }
//=====训练
  override def train(inputData: Array[T with Object], inputColIndex: Array[Integer], labelColIndex: Array[Integer]): Integer = {
    try{
      val inputRows = inputData.asInstanceOf[Array[Row]] //数据格式转换
      val schema = inputRows(0).schema  //取第一行的结构信息
      val rdd = spark.sparkContext.makeRDD(inputRows)  //转化为rdd
      var df_org = spark.createDataFrame(rdd,schema).repartition(1) //用rdd和结构信息构造dataframe
      var df_org_colnames = df_org.columns
      //删除存在为0 的数据
      //========================================================================================
      df_org.na.fill(0.0)
      import spark.implicits._
      df_org.cache()
      df_org.show()
      println("训练集样本数:"+df_org.filter(_.toSeq.contains(0.0)).count())
      val df = df_org.filter(!_.toSeq.contains(0.0))   //去掉空值
      df.cache()
      df.show()

      //==单指标预警阈值========================================================================
      var arr1 = new ArrayBuffer[(String,Double,Double)]()
      for(colName2<-df.columns if colName2!="time") {
         var q1 = 2.5*df.select(colName2).repartition(1).orderBy(colName2).take(ceil((df.count())*0.25).toInt).last.getDouble(0) -1.5* df.select(colName2).repartition(1).orderBy(colName2).take(ceil((df.count())*0.75).toInt).last.getDouble(0)
         var q3 = 2.5*df.select(colName2).repartition(1).orderBy(colName2).take(ceil((df.count())*0.75).toInt).last.getDouble(0) -1.5* df.select(colName2).repartition(1).orderBy(colName2).take(ceil((df.count())*0.25).toInt).last.getDouble(0)


        arr1.append((colName2,q3,q1))

      }

     //各单指标预警阈值
     var df_rules_yj = arr1.toList.toDF("name","q3","q1")

      val df_rules_schema = df_rules(0).schema  //取第一行的结构信息
      val df_rules_rdd = spark.sparkContext.makeRDD(df_rules)  //转化为rdd
      var df_rulesed = spark.createDataFrame(df_rules_rdd,df_rules_schema).repartition(1) //用rdd和结构信息构造dataframe
    //  println("================++++++++=========================================")
      var df_rules_yj_a =df_rules_yj.join(df_rulesed,df_rules_yj("name")===df_rulesed("sensorID")).drop("sensorID","high","low").collect()

      var sing_warn = df_rules_yj_a.map(row =>{
        var ele = row.getAs[Int]("type")
      if (ele == 1){(row.getAs[String]("name"),1000000000.0,row.getAs[Double]("q1"))}
      else if(ele ==2){(row.getAs[String]("name"),row.getAs[Double]("q3"),-1000000000.0)}
      else if (ele == 3){(row.getAs[String]("name"),row.getAs[Double]("q3"),row.getAs[Double]("q1"))}
       }
      )

      sing_warn_t = sing_warn.toList.asInstanceOf[List[(String,Double,Double)]].toDF("name","q3","q1").rdd.collect().map(x=>(x.getString(0),Array(x.getDouble(1),x.getDouble(2))))

      var sing_warn_t_Json = json2MapWithLow(sing_warn_t)
      var trainData_sing = df
      for(colName2<-trainData_sing.columns if colName2!="time") {
        trainData_sing = trainData_sing.withColumn(colName2, when(col(colName2).>=(sing_warn_t_Json.get(colName2).get(0)) or (col(colName2).<=(sing_warn_t_Json.get(colName2).get(1))), 1)
          .when(col(colName2).<(sing_warn_t_Json.get(colName2).get(0)) and  (col(colName2).>(sing_warn_t_Json.get(colName2).get(1))),0))
      }

      val sing_va = new VectorAssembler()   //向量合并组装
        .setInputCols(trainData_sing.columns.filter(_!="time"))//将除了time列外的列合并成一个特征向量列（feartureVector)
        .setOutputCol("Vector")

      val sing_filteredDF = sing_va.transform(trainData_sing).filter{ row =>
        val vec = row.getAs[Vector]("Vector")
        val yuandian = Vectors.zeros(vec.size)
        Vectors.sqdist(yuandian, vec) > 0.0
      }.drop("Vector")

      //println("df",df.count())
      println("单指标预警次数：",sing_filteredDF.count())
      //sing_filteredDF.show(false)
      train_sing_anomalies = sing_filteredDF.collect()

      ///==单指标报警===========================================================
      var train_sing_fail =df

      val mapOfJson=  json2MapWithLow(param_rules)

      for(colName2<-train_sing_fail.columns if colName2!="time") {
        train_sing_fail = train_sing_fail.withColumn(colName2, when(col(colName2).>=(mapOfJson.get(colName2).get(0)) or (col(colName2).<=(mapOfJson.get(colName2).get(1))), 1)
          .when(col(colName2).<(mapOfJson.get(colName2).get(0)) and  (col(colName2).>(mapOfJson.get(colName2).get(1))),0))
      }

      val sing_fail_va = new VectorAssembler()   //向量合并组装
        .setInputCols(train_sing_fail.columns.filter(_!="time"))//将除了time列外的列合并成一个特征向量列（feartureVector)
        .setOutputCol("Vector")

      val sing_fail_filteredDF = sing_fail_va.transform(train_sing_fail).filter{ row =>
        val vec = row.getAs[Vector]("Vector")
        val yuandian = Vectors.zeros(vec.size)
        Vectors.sqdist(yuandian, vec) > 0.0
      }.drop("Vector")

      var sssss = danzhibjmx(sing_fail_filteredDF)
      var date_type= "yyyy/M/d HH:mm:ss"
      var bbbbbb= sssss.collect().map(x=>{(x.getAs[Int]("id_1"),transformat(x.getAs[Int]("startime").toLong.toString,date_type),transformat(x.getAs[Int]("endtime").toLong.toString,date_type))})
        .toList.toDF("id_1","startime","endtime")

      println("单指标报警次数："+bbbbbb.count())
      bbbbbb.show(bbbbbb.count().toInt,false)
      train_anomalies = bbbbbb.collect()




      ///==多指标预警===========================================================
      //根据样本个数计算最高k值
      val mSamples = df.count().toInt//样本个数

      val upLimit = math.sqrt(mSamples.toDouble/2).toInt  //计算

      var clusteringCost = 0.0  //聚类价值变量
      var finalCost = clusteringScore2(df,2)  //最终聚类价值
      var stopCon = 100.0   //终止变量
      var bestK = 100   //最优K值
      km_start_k =15
      km_by =2
      km_tol = 0.06

      //根据stopcon 停止条件（数据点距离中心点的距离和的迭代下降率，当距离和减少为10%以内是，停止迭代，确定最优k值）
      for(k<-km_start_k to upLimit by km_by){

        if(stopCon>km_tol){
          clusteringCost=clusteringScore2(df,k)
          stopCon=(finalCost-clusteringCost)/finalCost
          finalCost=clusteringCost
          bestK=k
          println(s"k:${k},cost:${clusteringCost}")
        }
        //println("k = :"+k)
      }
      ///============================
      println("Kmean最佳K值 :"+bestK)
      //建模流程，矢量化，标准化，kmeans模型训练
      val va = new VectorAssembler()   //向量合并组装
        .setInputCols(df.columns.filter(_!="time"))//将除了time列外的列合并成一个特征向量列（feartureVector)
        .setOutputCol("featureVector")

      val scaler = new StandardScaler()  //标准化
        .setInputCol("featureVector")
        .setOutputCol("scaledFeatureVector")

      val initMode = "k-means||"  //adopt the k-means ++ algorithm to calculate the initial clustering centers
      val km = new KMeans()   //创建Kmeans聚类方法
       // .setSeed(-938553939424533094l)
        .setInitMode(initMode)
        .setFeaturesCol("scaledFeatureVector")
        .setPredictionCol("cluster")
        .setK(bestK)
        .setTol(1.0e-5)
        .setMaxIter(40)
      val pipeLine = new Pipeline().setStages(Array(va,scaler,km))  //构建pipelinek
      val pipeLineModel = pipeLine.fit(df)                          //拟合数据
      val kmModel = pipeLineModel.stages.last.asInstanceOf[KMeansModel]  //把pipelineModel的最后阶段，即fit保存？
      //得到聚类中心点
      val centroids = kmModel.clusterCenters
      val clustered = pipeLineModel.transform(df)  //
      clustered.cache()


      val topAbnormal0 = ratio*clustered.count()
      val topAbnormal = topAbnormal0.toInt
      //println(topAbnormal)
      //
      var threshold = 0.0
      if(threshold_type == 1) {
        threshold = clustered. //
          select("cluster", "scaledFeatureVector").as[(Int, Vector)].
          map { case (cluster, vec) => Vectors.sqdist(centroids(cluster), vec) }. //计算各点到聚类中心的距离
          orderBy($"value".desc).take(topAbnormal).last //取前10个的最后一个，即第十个的值作为阈值  //理解错误，所有距离只有10个点


      }else if(threshold_type == 0){
        //依据3sigma的确定阈值
        val threshold1 = clustered. //
          select("cluster", "scaledFeatureVector").as[(Int, Vector)].
          map { case (cluster, vec) => Vectors.sqdist(centroids(cluster), vec) }.toDF()

        val m = threshold1.describe().select("value").take(2).last(0).toString.toDouble

        val std = threshold1.describe("value").select("value").take(3).last(0).toString.toDouble
        threshold = m+3*std

      }
      println(s"距离阈值确定方式：${threshold_type};阈值=${threshold}")

      val originalCols = df.columns
      val train_anomalies1 = clustered.filter { row =>
        val cluster = row.getAs[Int]("cluster")
        val vec = row.getAs[Vector]("scaledFeatureVector")
        Vectors.sqdist(centroids(cluster), vec) >= threshold  //距离大于离群点标准
      }.select(originalCols.head, originalCols.tail:_*)


      ///==结果合并====================================================
      println("单指标预警次数："+sing_filteredDF.count)
      println("单指标报警次数："+bbbbbb.count())
      println("多指标预警次数："+train_anomalies1.count)

//
      m_model=pipeLineModel
      threshold_global = threshold
      0
    }catch {
      case e:Exception=>
        e.printStackTrace()
        -1
    }
  }

  override def saveModel(modelFilePath: String): Integer = {
    //保存模型参数以及确定好的离群标准
    try{
      m_model.write.overwrite().save(modelFilePath)  //保存模型
      var bos = new FileOutputStream(modelFilePath + "ci")  //？
//      var bos = new FileOutputStream("/home/biop/data/wyz/Abnormaldetetion/save/" + "ci")
      var oos = new ObjectOutputStream(bos)  //
      oos.writeObject(threshold_global)
      oos.flush()
      oos.close()
      bos.flush()
      bos.close()

      bos = new FileOutputStream(modelFilePath + "ci2")
//      bos = new FileOutputStream("/home/biop/data/wyz/Abnormaldetetion/save/" + "ci2")
      oos = new ObjectOutputStream(bos)
      //保存划分中心点和附属数据个数
      oos.writeObject(train_anomalies)
      oos.flush()
      oos.close()
      bos.flush()
      bos.close()

      bos = new FileOutputStream(modelFilePath + "ci3")
//      bos = new FileOutputStream("/home/biop/data/wyz/Abnormaldetetion/save/" + "ci3")
      oos = new ObjectOutputStream(bos)
      //保存划分中心点和附属数据个数
      oos.writeObject(sing_warn_t)
      oos.flush()
      oos.close()
      bos.flush()
      bos.close()

      0
    }catch {
      case e:Exception=>
        e.printStackTrace()
        -1
    }
  }

  override def loadModel(modelFilePath: String): Integer = {
    //加载模型以及离群标准
    try{
      m_model = PipelineModel.load(modelFilePath)
      var bis = new FileInputStream(modelFilePath + "ci")
//      var bis = new FileInputStream("/home/biop/data/wyz/Abnormaldetetion/save/"+ "ci")
      var ois = new ObjectInputStream(bis)
      threshold_global = ois.readObject.asInstanceOf[Double]
      println("threshold: "+threshold_global)
      ois.close()
      bis.close()
      bis = new FileInputStream(modelFilePath + "ci2")
//      bis = new FileInputStream("/home/biop/data/wyz/Abnormaldetetion/save/" + "ci2")    //集群
      ois = new ObjectInputStream(bis)
      train_anomalies =ois.readObject.asInstanceOf[Array[AnyRef]].map(x=>x.asInstanceOf[Row])

      ois.close()
      bis.close()
      println("LOAD_train_anomalies")
      val schema = train_anomalies(0).schema  //取第一行的结构信息
      val rdd = spark.sparkContext.makeRDD(train_anomalies)  //转化为rdd
      var df = spark.createDataFrame(rdd,schema).repartition(1).sort("id_1") //用rdd和结构信息构造dataframe
      df.show(false)  //数据缓存

      bis = new FileInputStream(modelFilePath + "ci3")
//      bis = new FileInputStream("/home/biop/data/wyz/Abnormaldetetion/save/" + "ci3")    //集群
      ois = new ObjectInputStream(bis)
      sing_warn_t =ois.readObject.asInstanceOf[Array[(String,Array[Double])]]

      ois.close()
      bis.close()

      0
    }catch {
      case e:Exception=>
        e.printStackTrace()
        -1
    }
  }

  override def predict(inputData: Array[T with Object]): Array[T with Object] = {
    //读取测试数据
    try{
      val inputRows = inputData.asInstanceOf[Array[Row]]
      val schema = inputRows(0).schema
      val rdd = spark.sparkContext.makeRDD(inputRows)
      var df = spark.createDataFrame(rdd,schema).repartition(1)
      df = df.na.fill(0.0)
      df.cache()
      var anomalies1 = spark.emptyDataFrame
      if(df.na.fill(0.0).filter(_.toSeq.contains(0.0)).count() == 1){
        println("This data is illegal and the sensor contains a value of 0")
        anomalies1 = df

      }else{

        //==单指标报警
        var train_sing_fail =df

        //==================================================================================@
            //====================================================================================@
        val mapOfJson=  json2MapWithLow(param_rules)

        for(colName2<-train_sing_fail.columns if colName2!="time") {
          train_sing_fail = train_sing_fail.withColumn(colName2, when(col(colName2).>=(mapOfJson.get(colName2).get(0)) or (col(colName2).<=(mapOfJson.get(colName2).get(1))), 1)
            .when(col(colName2).<(mapOfJson.get(colName2).get(0)) and  (col(colName2).>(mapOfJson.get(colName2).get(1))),0))
        }

        val sing_fail_va = new VectorAssembler()   //向量合并组装
          .setInputCols(train_sing_fail.columns.filter(_!="time"))//将除了time列外的列合并成一个特征向量列（feartureVector)
          .setOutputCol("Vector")

        val sing_fail_filteredDF = sing_fail_va.transform(train_sing_fail).filter{ row =>
          val vec = row.getAs[Vector]("Vector")
          val yuandian = Vectors.zeros(vec.size)
          Vectors.sqdist(yuandian, vec) > 0.0
        }.drop("Vector")


        sing_fail_filteredDF.cache()  //数据缓存



        //===============
        if (sing_fail_filteredDF.count() == 1) {
          println("单指标报警.")
          anomalies1 = sing_fail_filteredDF
        }else {
          //读取模型并对测试集进行聚类并得出聚类结果
          val pipeLineModel = m_model
          val kmModel = pipeLineModel.stages.last.asInstanceOf[KMeansModel]//？

          val predictedCluster = pipeLineModel.transform(df)   //聚类的预测是啥？
          //predictedCluster.select("cluster").groupBy("cluster").count().orderBy($"cluster",$"count".desc).show()//将测试数据按之前的聚类进行划分？
          val cost = kmModel.computeCost(pipeLineModel.transform(df))/df.count()


          //得到聚类中心点以及用离群标准计算出所有的离群点，并输出
          val centroids = kmModel.clusterCenters
          val originalCols = df.columns

          val anomalies = predictedCluster.filter { row =>
            val cluster = row.getAs[Int]("cluster")
            val vec = row.getAs[Vector]("scaledFeatureVector")
            Vectors.sqdist(centroids(cluster), vec) >= threshold_global  //距离大于离群点标准
          }.select(originalCols.head, originalCols.tail:_*)
          anomalies.cache()

          //==单指标预警

          var sing_warn_t_Json = json2MapWithLow(sing_warn_t)
          var trainData_sing = df
          for(colName2<-trainData_sing.columns if colName2!="time") {
            trainData_sing = trainData_sing.withColumn(colName2, when(col(colName2).>=(sing_warn_t_Json.get(colName2).get(0)) or (col(colName2).<=(sing_warn_t_Json.get(colName2).get(1))), 1)
              .when(col(colName2).<(sing_warn_t_Json.get(colName2).get(0)) and  (col(colName2).>(sing_warn_t_Json.get(colName2).get(1))),0))
          }

          val sing_va = new VectorAssembler()   //向量合并组装
            .setInputCols(trainData_sing.columns.filter(_!="time"))//将除了time列外的列合并成一个特征向量列（feartureVector)
            .setOutputCol("Vector")

          val sing_filteredDF = sing_va.transform(trainData_sing).filter{ row =>
            val vec = row.getAs[Vector]("Vector")
            val yuandian = Vectors.zeros(vec.size)
            Vectors.sqdist(yuandian, vec) > 0.0
          }.drop("Vector")




          if(anomalies.count() == 1 && sing_filteredDF.count() == 1){

            println("即单指标预警又是多指标预警.")
            anomalies1 = sing_filteredDF
          }else if (anomalies.count() == 0 && sing_filteredDF.count() == 1){
            println("单指标预警. ")
            anomalies1 = sing_filteredDF

          }else if(anomalies.count() == 1 && sing_filteredDF.count() == 0){
            println("多指标预警. ")
            anomalies1 = anomalies

          }else{
            println("正常 ")
            anomalies1 = df
          }

        }

      }

      var da= anomalies1.collect().map(x=>{(x.getAs[Int]("time"),transformat(x.getAs[Int]("time").toLong.toString,"yyyy/M/d HH:mm:ss"))}).toList.toDF("time1","timenew")
      da.join(anomalies1,anomalies1("time")===da("time1")).drop("time","time1").show()


      anomalies1.collect().asInstanceOf[Array[T with Object]]


    }catch {
      case e:Exception=>
        e.printStackTrace()
        null
    }
  }

  override def getRMSE(inputData: Array[T with Object], labelColIndex: Integer): java.lang.Double = {
    var predictionRate = 0.0
    try{
      val inputRows = inputData.asInstanceOf[Array[Row]]//testdata
      val schema = inputRows(0).schema
      val rdd = spark.sparkContext.makeRDD(inputRows)
      var df = spark.createDataFrame(rdd,schema).repartition(1).na.fill(0.0)//test.data
      df.cache()
      var df_normal = df.filter(!_.toSeq.contains(0.0))
      val pipeLineModel = m_model
      val kmModel = pipeLineModel.stages.last.asInstanceOf[KMeansModel]
      val predictedCluster = pipeLineModel.transform(df_normal)
      predictedCluster.select("cluster").groupBy("cluster").count().orderBy($"cluster",$"count".desc).show()
      val cost = kmModel.computeCost(pipeLineModel.transform(df_normal))/df_normal.count()  //cost指的啥？
      println("clustering cost: "+cost)

      val centroids = kmModel.clusterCenters  //聚类中心
      val originalCols = df.columns           //testdata的列名
     // println("originalCols:"+originalCols.mkString(","))
      //异常点数据记录
      val anomalies = predictedCluster.filter { row =>
        val cluster = row.getAs[Int]("cluster")
        val vec = row.getAs[Vector]("scaledFeatureVector")
        Vectors.sqdist(centroids(cluster), vec) >= threshold_global
      }.select(originalCols.head, originalCols.tail:_*)
      anomalies.cache()
      println(s"多指标预警： ${anomalies.count()}")


      ///单指标预警=========================================================

      var sing_warn_t_Json = json2MapWithLow(sing_warn_t)
      var trainData_sing = df_normal
      for(colName2<-trainData_sing.columns if colName2!="time") {
        trainData_sing = trainData_sing.withColumn(colName2, when(col(colName2).>=(sing_warn_t_Json.get(colName2).get(0)) or (col(colName2).<=(sing_warn_t_Json.get(colName2).get(1))), 1)
          .when(col(colName2).<(sing_warn_t_Json.get(colName2).get(0)) and  (col(colName2).>(sing_warn_t_Json.get(colName2).get(1))),0))
      }

      val sing_va = new VectorAssembler()   //向量合并组装
        .setInputCols(trainData_sing.columns.filter(_!="time"))//将除了time列外的列合并成一个特征向量列（feartureVector)
        .setOutputCol("Vector")

      val sing_filteredDF = sing_va.transform(trainData_sing).filter{ row =>
        val vec = row.getAs[Vector]("Vector")
        val yuandian = Vectors.zeros(vec.size)
        Vectors.sqdist(yuandian, vec) > 0.0
      }.drop("Vector")

      //println("",df.count())
      println("单指标预警："+sing_filteredDF.count())
      sing_filteredDF.cache()
      //=====================================

      var anomal_df1 = anomalies.select("time").except(sing_filteredDF.select("time"))
      println("多指标预警且不是单指标预警："+anomal_df1.count())
      var anomal_df2 =sing_filteredDF.select("time").except(anomalies.select("time"))
      println("单指标预警且不是多指标预警："+anomal_df2.count())
      var anomal_df3 =anomalies.select("time").intersect(sing_filteredDF.select("time"))
      println("即是多指标预警又是单指标预警："+anomal_df3.count())
      var anomal_df = (anomal_df1.union(anomal_df2)).union(anomal_df3)
      println("多指标预警和单指标预警并集（预警）："+anomal_df.count())
      anomal_df.cache()

      //
     // println("========================单指标===========================")
      //val predictionRate2 =getPredictionRate(sing_filteredDF,df)
      //println("========================多指标===========================")
      //val predictionRate1 =getPredictionRate(anomalies,df)
      //以上与预测模块相同，这里再加上预测准确率，并输出。
      //println("========================一起===========================")
      predictionRate =getPredictionRate(anomal_df,df)
      println("predictionRate: "+predictionRate)
      0
    }catch {
      case e:Exception=>
        e.printStackTrace()
        -1
    }
    predictionRate
  }

  def clusteringScore2(data: DataFrame, k: Int): Double = {
    //计算每一个k值所得到的聚类的结果，产生的所有数据点与其最近的中心点的距离和。
    val assembler = new VectorAssembler().
      setInputCols(data.columns).
      setOutputCol("featureVector")
    val d = Random.nextLong()

    val kmeans = new KMeans()
      //.setSeed(-938553939424533094l)
      .setK(k).
      setInitMode("k-means||").
      setPredictionCol("cluster").
      setFeaturesCol("featureVector").
      setMaxIter(40).
      setTol(1.0e-5)
    val pipeline = new Pipeline().setStages(Array(assembler, kmeans))
    val pipelineModel = pipeline.fit(data)
    val kmeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]
    kmeansModel.computeCost(pipelineModel.transform(data)) / data.count()
  }


  //计算预测准确率
  def getPredictionRate(abnormalDF:DataFrame,df:DataFrame): Double ={
    //abnormalDF 异常数据   df 测试数据
    //根据判定的离群点，确定在未来10min中之内发生单指标报警则为预测准确。
    var trainData = df.filter(!_.toSeq.contains(0.0))
    df.cache()

    val mapOfJson=  json2MapWithLow(param_rules)

    for(colName2<-trainData.columns if colName2!="time") {
      trainData = trainData.withColumn(colName2, when(col(colName2).>=(mapOfJson.get(colName2).get(0)) or (col(colName2).<=(mapOfJson.get(colName2).get(1))), 1)
        .when(col(colName2).<(mapOfJson.get(colName2).get(0)) and  (col(colName2).>(mapOfJson.get(colName2).get(1))),0))
    }

    val va = new VectorAssembler()   //向量合并组装
      .setInputCols(trainData.columns.filter(_!="time"))//将除了time列外的列合并成一个特征向量列（feartureVector)
      .setOutputCol("Vector")

    val traindatavec = va.transform(trainData).filter{ row =>
      val vec = row.getAs[Vector]("Vector")
      val yuandian = Vectors.zeros(vec.size)
      Vectors.sqdist(yuandian, vec) > 0.0
    }

    val filteredDF = traindatavec
    filteredDF.cache()  //数据缓存

    //println(s"Actually Alarmed Records: ${filteredDF.count()}")  //打印有单指标异常的个数
//============================================================================================
    val test = va.transform(trainData).map(
      row =>{
        val vec = row.getAs[Vector]("Vector")
        val yuandian = Vectors.zeros(vec.size)
        if(Vectors.sqdist(yuandian, vec) > 0.0){(row.getAs[Int]("time"),1)}
        else{(row.getAs[Int]("time"),0)}
      }
    ).toDF("time","type")
    //println("test======================================================")
    test.groupBy("type").count().show()
    var sssss= danzhibjmx(test)

    //sssss.show(false)
    println("单指标报警次数："+sssss.count())
    var date_type= "yyyy/M/d HH:mm:ss"
    var bbbbbb= sssss.collect().map(x=>{(x.getAs[Int]("id_1"),transformat(x.getAs[Int]("startime").toLong.toString,date_type),transformat(x.getAs[Int]("endtime").toLong.toString,date_type))})
      .toList.toDF("id_1","startime","endtime")
    bbbbbb.show(bbbbbb.count().toInt,false)

    //========================================================================================


    var startTime = 0  //开始时间
    var endTime = 0   //结束时间

    val timeWindowCon = timeWindow  //600秒，10分钟？按时间戳?????????
    var isAbnormal=0   //是否为异常
    var timeBlockDF = spark.emptyDataFrame  //空的数据框
    var timeBlockDF3 = spark.emptyDataFrame  //空的数据框
    var newArray = Array.empty[Row]      //空数组
    var newArray3 = Array.empty[Row]      //空数组

    ///
    var abnormalDF1 = (abnormalDF.select("time"))//.except(filteredDF.select("time"))

    //println ("abnormalDF1",abnormalDF1.count)
    //println("sing",(abnormalDF.select("time")).intersect(filteredDF.select("time")).count())
    abnormalDF1.cache()

    val abnormalArray = abnormalDF1.collect().map{row=>
      startTime=row(0).asInstanceOf[Int]
      endTime=startTime+timeWindowCon
      timeBlockDF = df.filter($"time">=startTime and $"time"<=endTime).select("time")
      newArray =timeBlockDF.collect().diff(filteredDF.select("time").collect())//集合取差？
      if(timeBlockDF.count().toInt>newArray.length){
        isAbnormal=1
      }else{
        isAbnormal=0
      }
      isAbnormal
    }
   println("准确率="+abnormalArray.sum.toDouble/abnormalArray.length ) //准确率
   println("误报率="+( 1-abnormalArray.sum.toDouble/abnormalArray.length)) //误报率

    ///=====================================================
    val abnormalArray3 = sssss.collect().map{row=>
      endTime=row.getAs[Int]("endtime")//-timeWindowCon
      startTime=row.getAs[Int]("startime")-timeWindowCon
      timeBlockDF3 = df.select("time").filter($"time">=startTime and $"time"<=endTime)
      newArray3 =timeBlockDF3.collect().diff(abnormalDF1.collect())//集合取差？
      if(timeBlockDF3.count.toInt>newArray3.length){
        isAbnormal=1
      }else{
        isAbnormal=0
      }
      isAbnormal
    }
    println("漏报率="+( 1-abnormalArray3.sum.toDouble/abnormalArray3.length)) //误报率
    abnormalArray.sum.toDouble/abnormalArray.length
  }
  def json2MapWithLow(arr:Array[(String,Array[(scala.Double)])]):mutable.HashMap[String,Array[(scala.Double)]]= {
    val map = new mutable.HashMap[String, Array[(scala.Double)]]
    for (i <- arr) {
      map.put(i._1,i._2)
    }
    map
  }
  def danzhibjmx(abnormalDF:DataFrame):DataFrame ={
    import spark.implicits._

    var testData =abnormalDF.repartition(1).sort("time").cache()

    var df111= testData.take(testData.count().toInt-1).zipWithIndex.map(x=>(x._1.getAs[Int](0),x._1.getAs[Int](1),x._2)).toList.toDF("time","type","id")

    var df222=testData.except(testData.limit(1)).sort("time").collect().zipWithIndex.map(x=>(x._1.getAs[Int](0),x._1.getAs[Int](1),x._2)).toList.toDF("time1","type1","id1")  //except后顺序发生变化

    var reslut = df222.repartition(1).sort("time1").join(df111.repartition(1).sort("time"),df222("id1")===df111("id")).drop("id","id1","time").select($"time1" as "time",$"type"*2+$"type1" as "typenew").repartition(1)

    var firsttype = reslut.repartition(1).sort("time").select("typenew").first.getInt(0)

    var lasttype = reslut.repartition(1).sort("time").select("typenew").take(reslut.count().toInt).last.getInt(0)

    var result_1 = reslut.filter("typenew = 1").repartition(1).sort("time")
    var result_2 = reslut.filter("typenew = 2").repartition(1).sort("time")
    var result_2_0 = spark.createDataFrame(Seq(( -1, 2))).toDF("time","typenew")
    var result_1_0 = spark.createDataFrame(Seq((-1, 1))).toDF("time","typenew")

    var relult_end = spark.emptyDataFrame
    if((firsttype ==0 || firsttype == 1) ){
      if(lasttype ==2 ||lasttype ==0){
        var b_1 =result_1.select("time").collect().zipWithIndex.map(x=>(x._1.getAs[Int](0),x._2)).toList.toDF("startime","id_1").repartition(1).sort("id_1")
        var b_2 =result_2.select("time").collect().zipWithIndex.map(x=>(x._1.getAs[Int](0),x._2)).toList.toDF("endtime","id_2").repartition(1).sort("id_2")
        relult_end=b_1.join(b_2,b_1("id_1")===b_2("id_2")).drop("id_2").select("id_1","startime","endtime").repartition(1).sort("id_1")

      }else{
        result_2= result_2.union(result_2_0).repartition(1)
        var b_1 =result_1.select("time").collect().zipWithIndex.map(x=>(x._1.getAs[Int](0),x._2)).toList.toDF("startime","id_1").repartition(1).sort("id_1")
        var b_2 =result_2.select("time").collect().zipWithIndex.map(x=>(x._1.getAs[Int](0),x._2)).toList.toDF("endtime","id_2").repartition(1).sort("id_2")
        relult_end =b_1.join(b_2,b_1("id_1")===b_2("id_2")).drop("id_2").select("id_1","startime","endtime").repartition(1).sort("id_1")
      }
    }else{

      if(lasttype ==2 ||lasttype ==0){
        result_1= result_1_0.union(result_1).repartition(1)
        var b_1 =result_1.select("time").collect().zipWithIndex.map(x=>(x._1.getAs[Int](0),x._2)).toList.toDF("startime","id_1").repartition(1).sort("id_1")
        var b_2 =result_2.select("time").collect().zipWithIndex.map(x=>(x._1.getAs[Int](0),x._2)).toList.toDF("endtime","id_2").repartition(1).sort("id_2")
        relult_end=b_1.join(b_2,b_1("id_1")===b_2("id_2")).drop("id_2").select("id_1","startime","endtime").repartition(1).sort("id_1")
      }else{
        result_1= result_1_0.union(result_1).repartition(1)
        result_2= result_2.union(result_2_0).repartition(1)
        var b_1 =result_1.select("time").collect().zipWithIndex.map(x=>(x._1.getAs[Int](0),x._2)).toList.toDF("startime","id_1").repartition(1).sort("id_1")
        var b_2 =result_2.select("time").collect().zipWithIndex.map(x=>(x._1.getAs[Int](0),x._2)).toList.toDF("endtime","id_2").repartition(1).sort("id_2")
        relult_end=b_1.join(b_2,b_1("id_1")===b_2("id_2")).drop("id_2").select("id_1","startime","endtime").repartition(1).sort("id_1")
      }

    }
    relult_end.sort("id_1")

  }

  def transformat(date:String,pattern:String):String ={
    val myformat = new SimpleDateFormat(pattern)
    //myformat.setTimeZone(TimeZone.getTimeZone("GMT"))
    val time=new Date(Long.valueOf(date)*1000L)
    myformat.format(time)}
}
