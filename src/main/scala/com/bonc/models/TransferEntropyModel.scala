package com.bonc.models

import java.io._
import java.lang

import com.bonc.IServices.computeTE
import com.bonc.interfaceRaw.IModel
//import com.bonc.Interface.IModel
import com.bonc.serviceImpl.GaussianTE
import net.sf.json.JSONObject
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.storage.StorageLevel
//cl：注意，包名不要和类名冲突，否则编译报错

import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.mllib.linalg.Vectors

import com.bonc.utils.graphUtils._
import com.bonc.utils.newUtils._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, _}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import scala.collection.mutable

/**
  * Created by sid on 2018/6/7 0007.
  */



//
//class TransferEntropyModel[T] extends IModel[T with Object] with java.io.Serializable {
//  //保存参数，序列化至本地
//  case class Params(localArray: Array[Row], schema: StructType, params: String)
//  //  val m_logger = new SysLogger(this)
//  var trainSet: Array[Row] = _ //String，接收的数据，直接为Arrow[Row] ，若是传CSV，也是Array[Row]
//  var outputPath = "" //作为模型保存路径
//
//  //传递熵参数
//  var k_max: Int = 0 //最大k值， 定阶用 ，传递熵参数
//  var l_max: Int = 0 //最大l值，传递熵参数
//  var delay_max: Int = 0 //最大delay值，传递熵参数
//  var pValue: Double = 1.0 //传递熵参数， 概率值，筛选保留<pValue的值，默认1.0为不筛选
//  //默认参数
//  var base: Int = 3 //传递熵的基——离散型计算需要传参，连续型不需要
//  var k_tau: Int = 1 //T(Y->X)中X过程的时延步长
//  var l_tau: Int = 1 //T(Y->X)中Y过程的时延步长
//  var model: computeTE = new GaussianTE()
//
//  //时序定阶参数
//  var maxP: Int = 10 //AR(P)拟合最大P阶
//  var maxD: Int = 0 //ARIMA(P,D,Q)拟合最大阶D
//  var maxQ: Int = 10 //ARIMA(P,D,Q）拟合最大阶Q
//  //    val partitionNum:Int = 0
//
//  //  传递熵的阀值
//  var threshold: Double = 0.1
//  //其他
//
//  //之前是var
//  @transient
//  val conf:SparkConf = new SparkConf().setAppName("TEcompute").set("spark.shuffle.consolidateFiles", "true") //.setJars(Array("sparkts-0.4.0-SNAPSHOT-jar-with-dependencies.jar"))
//  @transient
//  val sparkContext: SparkContext = SparkContext.getOrCreate(conf)
//  @transient
//  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
//  var parallelism: Int = 24 //并行度
//
//  var MiddleResult: DataFrame = _ //存储训练结果
//  var localMiddleResult: Array[Row] = _ //存储结果至本地
//  var schemaOfResult: StructType = _
//  //nblab参数
//  var inputColIndex: Array[Integer] = _
//  var outputColIndex: Array[Integer] = _
//  var jsonStr = ""
//
//  def getRMSE(ts: Array[T with Object], integer: Integer): lang.Double = 0.0
//
//  def setParam(params: String): Integer = {
//    try {
//      val paramJson = JSONObject.fromObject(params)
//      //提供给def predict
//      jsonStr = params
//
//      k_max = paramJson.getString("k_max").toInt
//      l_max = paramJson.getString("l_max").toInt
//      delay_max = paramJson.getString("delay_max").toInt
//      pValue = paramJson.getString("pValue").toDouble
//      threshold = paramJson.getString("threshold").toDouble
//      base = paramJson.getString("base").toInt
//      k_tau = paramJson.getString("k_tau").toInt
//      l_tau = paramJson.getString("l_tau").toInt
//      maxP = paramJson.getString("maxP").toInt
//      parallelism = paramJson.getString("parallelism").toInt
//      inputColIndex = paramJson.getJSONArray("inputColIndex").toArray.map(x => Integer.valueOf(x.toString.toInt))
//      outputColIndex = paramJson.getJSONArray("outputColIndex").toArray.map(x => Integer.valueOf(x.toString.toInt))
//    } catch {
//      case e: IOException => {
//        e.printStackTrace()
//        //        m_logger.logErr(e.getMessage);
//        return -1
//      }
//      case e: Exception => {
//        e.printStackTrace()
//        //        m_logger.logErr(e.getMessage);
//        return -1
//      }
//      case e: Throwable => {
//        e.printStackTrace()
//        //        m_logger.logErr(e.getMessage);
//        return -1
//      }
//    }
//
//    0
//  }
//
//  def loadModel(savePath: String): Integer = {
//    println("loadModel stage--------------------------------------")
//    //    m_gmModel = GaussianMixtureModel.load(modelFilePath)
//    val bis = new FileInputStream(savePath + "ci")
//    val ois = new ObjectInputStream(bis)
//    val params_loaded = ois.readObject.asInstanceOf[Params]
//    //解析json参数
//    setParam(params_loaded.params)
//    //解析中间结果
//    localMiddleResult = params_loaded.localArray
//    schemaOfResult = params_loaded.schema
//    ois.close()
//    bis.close()
//    0
//  }
//
//
//  //cl:序列化到本地：保存中间结果
//  def saveModel(savePath: String): Integer = {
//    println("saveModel stage----------------------------------!")
//
//    localMiddleResult = MiddleResult.collect()
////    localMiddleResult.foreach(println)
//    schemaOfResult = MiddleResult.schema
//    //中间结果均需要写到本地，勿写到hdfs
////    MiddleResult.coalesce(1).write.format("csv").mode(SaveMode.Overwrite).save("file://" + savePath) //需要上传至本地文件
//    //写到hdfs，报错
////    MiddleResult.coalesce(1).write.format("csv").mode(SaveMode.Overwrite).save("hdfs://172.16.32.139:9000/usr/cl")
//    //写到本地  savePath\ci\或 savePath/ci/
//    val bos = new FileOutputStream(savePath + "ci");
////    val bos = new FileOutputStream(savePath +System.lineSeparator()+ "ci");
//    val oos = new ObjectOutputStream(bos)
//
//    //保存参数的case class
//    val params1 = Params(localMiddleResult, schemaOfResult, jsonStr)
//    oos.writeObject(params1)
//    oos.flush()
//    oos.close()
//    bos.flush()
//    bos.close()
//    0
//  }
//
//  //predictSet是指用户要传的那列传感器，目前仅支持1列
//  //@warn:不继承接口
//  //  def predict(predictSet: Array[Row]): Array[Row] = {
//
//  //继承接口
//  def predict(predictSetRaw: Array[T with Object]): Array[T with Object] = {
//
//    val predictSet = predictSetRaw.asInstanceOf[Array[Row]]
//    //要生成二层网络的传递熵值
//    //用户传进某传感器，获取该传感器的列名
//
//    //cl:获取用户要传的传感器，生成全部指向该传感器的“二层网络图”
//    //    val  sensorName = predictSet(0).getString(0)
//
//    //目前只能传1列传感器，故取第1列第1行的字段名（其实也可以实现传多列，也就是对多个节点产生“二层网络”）
//    val sensorName = predictSet(0).schema.fieldNames(0)
//    println("sensorName:" + sensorName + "--------------------------------")
//
//    val rdd_loaded = sparkContext.parallelize(localMiddleResult, parallelism)
//    val df_loaded = spark.createDataFrame(rdd_loaded, schemaOfResult)
//
//    println("df_loaded show:  use in python-------------------------------------------")
//    df_loaded.show(1000)
//
//    //输出给python展示（未筛选过的图）
//    println("pValue:"+pValue+"---------------------------------------------------------")
//    println("teValue:"+threshold+"-----------------------------------------------------")
//    df_loaded.coalesce(1).write.mode(SaveMode.Overwrite).csv("/usr/cl/transferEntropy/out/MiddleResult")
//
//    //cl:图的顶点
//    //此处MiddleResult不是指边，而只是中间结果
//    val nodes = df_loaded.select("srcToTarget").collect().map(x => x.getString(0).split("->")).flatMap(x => x).distinct//toSet
//    println("nodes: a set!--------------------------------------------")
//    nodes.foreach(println)
//
//    // 图的DataFrame形式（包含边三元组的所有信息）
//    val linksOfGraph = df_loaded.select("srcToTarget", "teValue", "pValue")
//      .withColumn("srcToTarget", split(col("srcToTarget"), "->"))
//      .withColumn("src", col("srcToTarget")(0))
//      //      生成含srcToTarget,src,target,teValue,pValue这几列的DF
//      .withColumn("target", col("srcToTarget")(1))
//      //      筛选teValue>te阀值的行
//      .where(col("teValue") > threshold)
//      //筛选pValue<p阀值的行
//      .where(col("pValue") < pValue)
//      .where("src != target")         //可能会影响到后面的
//      .drop("srcToTarget", "pValue")
//    println("links: a dataframe!-------------------------------------")
//    linksOfGraph.show(100)
//    //    @cl-0612s
//
//    //获取某顶点的“逆向”的邻接列表（DataFrame中指向target节点的所有顶点，可以使用“图”的边反转，再求“反转图”的邻接列表
//    //二层关系图同理：从target起，一层原因：“反转图”的邻接列表；二层原因：一层原因节点的”邻接列表”,使用pregel框架，可遍历N层，传入参数为迭代计算次数N
//    //@warn 考虑用graphx实现
//    val firstLayerDF = linksOfGraph.where(col("target").===(sensorName))
//    //获取“一层原因”的节点名称
//    val firstLayerNodes = firstLayerDF.select(col("src")).collect.map(x => x.getString(0))
//
//    println("first layer DF----------------------------------------------")
//    firstLayerDF.show()
//    println("first layer nodes----------------------------------------------")
//    firstLayerNodes.foreach(println)
//
//
//    //isin接收参数为list:Any*， 是可变参数列表（数组也可）
//    //    val secondLayerDF = linksOfGraph.where(col("src").isin(firstLayerNodes:_*))       //此处src应该为target
//    val secondLayerDF = linksOfGraph.where(col("target").isin(firstLayerNodes: _*))
//    println("sencondLayer: a dataframe!-------------------------------")
//    secondLayerDF.show(100)
//    println("second layer nodes----------------------------------------------")
//    val secondLayerNodes = secondLayerDF.select(col("src")).collect.map(x => x.getString(0))
//    secondLayerNodes.foreach(println)
//    //第一层和第二层unionr
//    val allLinks = firstLayerDF.union(secondLayerDF)
//      .where("src != target")
//    println("allLinks--------------------------")
//    allLinks.show()
//
//
//    //出度为0的src节点——DF中target分组计数，必然全不为0，所有节点去除这些出度非0节点
//    //也可以：收集所有src列元素：其中元素的出度必不为0；  收集所有target列元素，其中元素入度必不为0
//    val outDegree = linksOfGraph.groupBy("target").count().select("target").collect().map(x => x.getString(0))
//    val outDegreeZeroNodes = nodes.filterNot(outDegree.contains)
//
//    println("nodes: outdegree!=0---------------------------")
//    outDegreeZeroNodes.foreach(println)
//
//    val inDegreeNotZeroNodes = linksOfGraph.groupBy("src").count().select("src").collect().map(x => x.getString(0))
//    inDegreeNotZeroNodes.foreach(println)
//
//
//    val rootNodesPossible =outDegreeZeroNodes.intersect(inDegreeNotZeroNodes)
//    println("nodes: indegree!=0---------------------------")
//    //节点为根原因条件之一：入度为0，出度非0
//    println("nodes: root reason to be selected---------------------------")
//    println(rootNodesPossible.mkString(","))
//    //节点为根原因条件之二：root节点 到 target目标节点（指被分析的该节点），有连通路径
//    //逻辑暂缺：1.选出与所有目标节点“连通”的根原因；2.算法：计算二者的“所有连通路径”
//
//    //secondLayer.collect()     //貌似必须返回DataFrame，考虑返回经过筛选后的“图”——table形式
//
//
//    //    图计算部分,nodes:Array[String],links->linksOfGraph:DataFrame["src","target","teValue"]
//
//    //为顶点集nodes的元素添加VertexId:Long
//    val vertices = (0L to (nodes.length - 1).toLong).map(x => (x, nodes(x.toInt)))
//    val vertexRDD: RDD[(VertexId, String)] = sparkContext.parallelize(vertices)
//
//    //存放键值对：String->Long
//    val map1 = new mutable.HashMap[String, Long]()
//    vertices.foreach(x => map1.put(x._2, x._1))
//    println("打印map1------------------------------------")
//    println(map1)
//    //存放键值对：Long,String
//    val map2 = new mutable.HashMap[Long, String]()
//    vertices.foreach(x => map2.put(x._1, x._2))
//
//    val edgeRDD: RDD[Edge[Double]] = linksOfGraph
//      .select("src", "target", "teValue")
//      .rdd
//      .map(x => (x.getString(0), x.getString(1), x.getDouble(2)))
//      .map(x => Edge(map1.get(x._1).get, map1.get(x._2).get, x._3))
//
//    val defaultMissing = ("Uknown")
//    val graph = Graph(vertexRDD, edgeRDD, defaultMissing)
//
//
//    //获取“根原因”候选节点集合：入度为0、出度不为0的顶点集——图中入度/出度为0的顶点，不会出现在inDegrees/outDegrees得到的顶点
//    val indegreesNotZero = graph.inDegrees.map(x => x._1.toLong)
//    val indegreesZero = graph.vertices.map(x => x._1.toLong).subtract(indegreesNotZero)
//
//
//    //.foreach(println)   //得到indegrees==0的顶点的VertexId
//    val outDegreesNotZero = graph.outDegrees.map(x => (x._1.toLong, x._2))
//      .filter(x => x._2 > 0) //过滤得到度数量>1的顶点，而不是过滤得到顶点值>0的顶点 ，此处filter不用，默认只收集出度>0的顶点
//      .map(x => x._1)
//    val rootNodesCandidate = indegreesZero.intersection(outDegreesNotZero) //出度>0且入度=0的顶点VertexId集合，RDD[Long]
//      .collect()
//    println("入度为0的nodes---------------------------------")
//    println(indegreesZero.collect().mkString(","))
//    println(indegreesZero.map(x=>map2.get(x).get).collect().mkString(","))
//    println("入度不为0的点————————————-----------")
//    println(indegreesNotZero.collect().mkString(","))
//    println(indegreesNotZero.map(x=>map2.get(x).get).collect().mkString(","))
//    println("全部点————————————-----------")
//    println(graph.vertices.map(x=>x._1.toLong).collect().mkString(","))
//    println(graph.vertices.map(x=>map2.get(x._1.toLong).get).collect().mkString(","))
//
//
//    println("出度不为0的nodes---------------------------------")
//    println(outDegreesNotZero.collect().mkString(","))
//
//    println("graphx : root nodes possible!--------------------------")
//    println("long id:"+rootNodesCandidate.mkString(","))
//    println("name:"+rootNodesCandidate.map(x=>map2.get(x).get).mkString(","))
//    //计算连通分量,有缺陷,不使用
//    //    val cc = graph.connectedComponents()
//    //    //根节点VertexId若和选定的target节点，所在连通图的minVertexId一致，则保留，作为真正的“根原因”
//    //    //连通分量的所有（VertexId,VertexId）——即（graph顶点，graph的连通分量的Id）
//    //    val ccVerticesInfo = cc.vertices.map(x=>( x._1.toLong,x._2.toLong)).collect()
//    //    val hashMapCC = new mutable.HashMap[Long,Long]()
//    //    ccVerticesInfo.map(x=> hashMapCC.put( x._1,x._2))
//    //
//    //    //得到根原因节点的集合Array[Long]
//    //    val rootReasons = rootNodes.filter( x=> map1.get(sensorName).get==hashMapCC.get(x).get )
//    //
//    //    //根据map2得到根原因节点的集合的Array[String]
//    //    val rootReasonsSensorName = rootReasons.map(x=>map2.get(x).get)
//
//
//    //计算最短路径是否存在（即root节点是否能到达指定的target——ShortestPaths
//    val landmarks = Seq(map1.get(sensorName).get)
//    val arrsRoots = findShortestPaths(graph, landmarks)
//
//    println("可能根原因中,能到达target节点的根原因集合——————————————————--——————")
//    arrsRoots.foreach(println)
//    println(arrsRoots.map(x => map2.get(x).get).mkString(","))
//
//    println("目标节点的long id和SensorName-------------------------------------")
//    println(sensorName+":"+map1.get(sensorName).get)
//
//    println("root nodes filtered at last----------------------------------")
//    val rootNodesAtLast = rootNodesCandidate.intersect(arrsRoots)
//    println(rootNodesAtLast.map(x => x+"->"+map2.get(x).get).mkString(","))
//
//    //    图计算部分
//
//
//    //    allLinks.collect()
//    allLinks.collect().asInstanceOf[Array[T with Object]]
//
//  }
//
//  //cl:不继承接口
//  //  def train(trainData: Array[Row], inputColIndex: Array[Integer], outputColIndex: Array[Integer]): Integer = {
//
//
//  //使用Array[T]接口
//  def train(trainDataRaw: Array[T with Object], inputColIndex: Array[Integer], outputColIndex: Array[Integer]): Integer = {
//    val trainData = trainDataRaw.asInstanceOf[Array[Row]]
//    //df根据inputColIndex选取列的另一种方法，假设由Array[Row]生成df——好处在于不需要再用RDD构造，不需要重新定义schema
//    //则新df为 ：
//    // val newColumns = (0 to inputColIndex.length-1).map( x => col(df.columns(x)))
//    // val newDF = df.select(newColumns:_*)
//
//    val schemaString = "srcToTarget k l delay teValue pValue"
//    val fields = schemaString.split(" ")
//      .map(fieldName => StructField(fieldName, StringType, nullable = true))
//    val schema = StructType(fields)
//
//
//    //    val lines = sparkContext.textFile((inputFilePath),24)
//    //    val rdd = lines.map(x=>x.split(",")(0)+"@"+x.split(",").toBuffer.remove(0).toArray.mkString(","))
//    val schema1 = trainData(0).schema
//    val schema1Names = schema1.fieldNames
//
//    //模式匹配：如果inputColIndex为空，即长度为0，则选取所有列
//    val schemaNames1 = inputColIndex.length match {
//      case 0 => schema1Names;
//      case _ => inputColIndex.map(x => schema1Names(x)) //选取指定列,dataframe选取这些列作计算
//    }
//
//    //获取原schema的字段名->数据类型的映射
//    //    val fieldsMap = (0 to schema1.fieldNames.length-1).map(x => (schema1.fieldNames(x),schema1.fields.apply(x) ))
//    //    val schemaNames1 = inputColIndex.map(x => schema1Names(x))   //选取指定列,dataframe选取这些列作计算
//    //    val trainData1 = inputColIndex.map(x=>)
//    val rdd0 = sparkContext.parallelize(trainData)
//    val cols = schemaNames1.map(x => col(x))
//
//    val fieldsNew = (0 to schemaNames1.length - 1).map(x => schema1.fields.apply(x))
//    val schemaNew = StructType(fieldsNew)
//    //此处应该使用新的schema,而不是schema1,否则train和predict阶段的字段名会不一致
//    //    val trainData1 = spark.createDataFrame(rdd0,schema1)
//    val trainData0 = spark.createDataFrame(rdd0, schemaNew)
//      .select(cols: _*) //
//    //                         .collect()
//    //                          .drop(1)      //Array中丢弃某个元素，从1开始，而非从0开始
//    //cl:Dataframe转置
//
//    println("trainData0--------------------------------")
//    trainData0.show()
//    val trainData1 = trainData0.collect().drop(1)
//    //已经是转置了，Dataframe转置的过程——此处已实现，也可看DenseMatrix的transpose方法
//    //Matrix构造必须是Array[Double],未采用其转置
//    val arrs = {
//      0 to schemaNames1.length - 1
//    }.map(x => trainData1.map(y => y.getString(x)).mkString(",")).toArray
//    var arrs1 = {
//      0 to schemaNames1.length - 1
//    }.map(x => schemaNames1(x) + "," + arrs(x)).toArray
//    //scala上没错
//    println("arr1 value!")
////    arrs1.foreach(println)
//
//    //                        .map(x=>x.mkString(","))
//    //
//    //    val m_inputCnt = inputColIndex.size
//    //    val m_inputCols = new ArrayBuffer[String]()
//    ////    val rdd0 = sparkContext.parallelize(trainData)
//    //    for(i<-0 until inputColIndex.size){
//    //      m_inputCols.append(schema.fieldNames(inputColIndex(i)))
//    //    }
//    //    val sc = new SQLContext(m_sparkContext)
//
//
//    val trainSet = arrs1
//    //    val trainSet = trainData.map(_.mkString(","))
//
//    val rdd = sparkContext.parallelize(trainSet, parallelism)
//    rdd.cache()
//    println("train_point0")
//    val rdd1 = rdd.map(x => x.replaceFirst(",", "@"))
//
//    //此处广播变量不太适用
//    //    val cartesian_array = sparkContext.broadcast(rdd1.cartesian(rdd1).repartition(parallelism).collect())
//
//    val lines_cartesian = rdd1.cartesian(rdd1).repartition(parallelism)
//
//    //持久化策略
//    lines_cartesian.persist(StorageLevel.MEMORY_AND_DISK)
//
//    println("train_point1")
////    lines_cartesian.take(20).foreach(println)
//    //cl:时序定阶
//    //warn:这里耗时过长，2W3*16的数据，此处耗时5-7min，貌似是spark-ts库的并发锁导致，也有可能是collectAsMap导致，考虑使用别的方式collect
//    //时序定阶————注意Vector是scala自带的类型，Vector则是spark中自定义的类（非类型),且vector类所在包，有静态类Vectors，勿混淆
//    val map_ts_k = rdd1.map(x => (x.split("@")(0), findBestArmaP(Vectors.dense(x.split("@")(1).map(_.toDouble).toArray), maxP, 0, 0, k_max))
//      //这一句定阶：如果ARIMA中差分也无法平稳，则默认为K_MAX，只计算K_MAX的值
//      //      ( x.split("@")(0),findBestArmaP(Vectors.dense(x.split("@")(1).map(_.toDouble).toArray),maxP,maxD,maxQ,k_max   )         )      //findBestArmaP( DenseVector,maxP,maxD,maxQ )
//      //由于ARIMA考虑到时序的平稳性，若在maxD内都不平稳，会报错。为了定阶，AR就够了，可以考虑maxD和maxQ定阶为0
//      //k_max为防止上面ARIMA报错，给个默认的k值输出
//      //      ( x.split("@")(0),findBestArmaP(Vectors.dense(x.split("@")(1).map(_.toDouble).toArray),maxP,0,0,k_max   )         )
//    ).collectAsMap()
//
////    println(map_ts_k.get("AI7006").get)
//    println("train_point2")
//    val broadcastMap = sparkContext.broadcast(map_ts_k) //广播变量，缓存srcName->best_k的Map
//
//    //    val hashMap = new mutable.HashMap[String,Int]()
//    //    map_ts_k.map(x => hashMap.put(x._1,x._2))
//
//    print("stage hashMap done!")
//
//    //测试
//    //    lines_cartesian.take(5).foreach(println)
//
//    //cl:计算传递熵
//
//    //计算传递熵矩阵（未定阶）
//    val rowRDD = lines_cartesian
//      //            生成 4元元组 （srcName, srcSeries,destName,destSeries) ,其中，srcName,destName为String类型，表示source和target的变量名
//      //              srcSeries和destSeries是Array[Double]类型， 作为传递熵值计算的输入，两个 double[]类型的序列
//      .map(x => (x._1.split("@")(0), x._1.split("@")(1).split(",").map(_.toDouble).toArray, x._2.split("@")(0), x._2.split("@")(1).split(",").map(_.toDouble).toArray))
//      //      .map( x => (x._1+"->"+x._3 , computeTwoSeriesTEsStringResultGaussianSrcName (x._1,hashMap,x._2,x._4 ,3,k_max,k_tau,l_max,l_tau,delay_max) ) )
//      //将上面的hashMap:HashMap 修改为hashMap:Map类型
//
//      .map(x => (x._1 + "->" + x._3, computeTwoSeriesTEsStringResultGaussianSrcName(x._1, map_ts_k, x._2, x._4, 3, k_max, k_tau, l_max, l_tau, delay_max)))
////      .map(x => (x._1 + "->" + x._3, computeTwoSeriesTEsStringResultGaussianSrcName(x._1, broadcastMap.value, x._2, x._4, 3, k_max, k_tau, l_max, l_tau, delay_max)))
//      .flatMap(y => y._2.map(result => (y._1, result)))
//      //            .flatMap(y => y._2.map(b=> y._1+","+b )    )
//      .map(x => Array(x._1, x._2).mkString(",")) //此处全部用“，”分隔，后面构造dataframe需要
//
//      .map(_.split(","))
//      .map(attributes => Row(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4), attributes(5)))
//
////    rowRDD.foreach(x=>x.getString(0))
//
//    // Apply the schema to the RDD
//    var DF = spark.createDataFrame(rowRDD, schema)
//    //除srcToTarget列外，全转成Double
//    for (column <- DF.columns if !column.equalsIgnoreCase("srcToTarget")) {
//      DF = DF.withColumn(column, col(column).cast(DoubleType))
//    }
//
//
//    //cl:每两个时间序列X->Y ，求出Max( Te(X->Y) )
//
//    //分组取最大值——即分组求top1
//    val w = Window.partitionBy("srcToTarget").orderBy(col("teValue").desc)
//    //    降序排序
//    MiddleResult = DF.withColumn("rn", row_number().over(w)).where(col("rn") === 1).drop("rn") //计算边的邻接矩阵
//      .where(col("pValue") < pValue)
//
//    println("show middle result-------------------------------")
//    MiddleResult.show(1000)
//    0
//  }
//
//
//
//
//
//
//}


//object TransferEntropyModel{
//
//}













































class TransferEntropyModel[T] extends IModel[T with Object] with java.io.Serializable {
  //保存参数，序列化至本地
  case class Params(localArray: Array[Row], schema: StructType, params: String)
  //  val m_logger = new SysLogger(this)
  var trainSet: Array[Row] = _ //String，接收的数据，直接为Arrow[Row] ，若是传CSV，也是Array[Row]
  var outputPath = "" //作为模型保存路径

  //传递熵参数
  var k_max: Int = 0 //最大k值， 定阶用 ，传递熵参数
  var l_max: Int = 0 //最大l值，传递熵参数
  var delay_max: Int = 0 //最大delay值，传递熵参数
  var pValue: Double = 1.0 //传递熵参数， 概率值，筛选保留<pValue的值，默认1.0为不筛选
  //默认参数
  var base: Int = 3 //传递熵的基——离散型计算需要传参，连续型不需要
  var k_tau: Int = 1 //T(Y->X)中X过程的时延步长
  var l_tau: Int = 1 //T(Y->X)中Y过程的时延步长
  var model: computeTE = new GaussianTE()

  //时序定阶参数
  var maxP: Int = 10 //AR(P)拟合最大P阶
  var maxD: Int = 0 //ARIMA(P,D,Q)拟合最大阶D
  var maxQ: Int = 10 //ARIMA(P,D,Q）拟合最大阶Q
  //    val partitionNum:Int = 0

  //  传递熵的阀值
  var threshold: Double = 0.1
  //其他

  //之前是var
  @transient
  val conf:SparkConf = new SparkConf().setAppName("TEcompute").set("spark.shuffle.consolidateFiles", "true") //.setJars(Array("sparkts-0.4.0-SNAPSHOT-jar-with-dependencies.jar"))
  @transient
  val sparkContext: SparkContext = SparkContext.getOrCreate(conf)
  @transient
  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  var parallelism: Int = 24 //并行度

  var MiddleResult: DataFrame = _ //存储训练结果
  var localMiddleResult: Array[Row] = _ //存储结果至本地
  var schemaOfResult: StructType = _
  //nblab参数
  var inputColIndex: Array[Integer] = _
  var outputColIndex: Array[Integer] = _
  var jsonStr = ""

  def getRMSE(ts: Array[T with Object], integer: Integer): lang.Double = 0.0

  def setParam(params: String): Integer = {
    try {
      val paramJson = JSONObject.fromObject(params)
      //提供给def predict
      jsonStr = params

      k_max = paramJson.getString("k_max").toInt
      l_max = paramJson.getString("l_max").toInt
      delay_max = paramJson.getString("delay_max").toInt
      pValue = paramJson.getString("pValue").toDouble
      threshold = paramJson.getString("threshold").toDouble
      base = paramJson.getString("base").toInt
      k_tau = paramJson.getString("k_tau").toInt
      l_tau = paramJson.getString("l_tau").toInt
      maxP = paramJson.getString("maxP").toInt
      parallelism = paramJson.getString("parallelism").toInt
      inputColIndex = paramJson.getJSONArray("inputColIndex").toArray.map(x => Integer.valueOf(x.toString.toInt))
      outputColIndex = paramJson.getJSONArray("outputColIndex").toArray.map(x => Integer.valueOf(x.toString.toInt))
    } catch {
      case e: IOException => {
        e.printStackTrace()
        //        m_logger.logErr(e.getMessage);
        return -1
      }
      case e: Exception => {
        e.printStackTrace()
        //        m_logger.logErr(e.getMessage);
        return -1
      }
      case e: Throwable => {
        e.printStackTrace()
        //        m_logger.logErr(e.getMessage);
        return -1
      }
    }

    0
  }

  def loadModel(savePath: String): Integer = {
    println("loadModel stage--------------------------------------")
    //    m_gmModel = GaussianMixtureModel.load(modelFilePath)
    val bis = new FileInputStream(savePath + "ci")
    val ois = new ObjectInputStream(bis)
    val params_loaded = ois.readObject.asInstanceOf[Params]
    //解析json参数
    setParam(params_loaded.params)
    //解析中间结果
    localMiddleResult = params_loaded.localArray
    schemaOfResult = params_loaded.schema
    ois.close()
    bis.close()
    0
  }


  //cl:序列化到本地：保存中间结果
  def saveModel(savePath: String): Integer = {
    println("saveModel stage----------------------------------!")

    localMiddleResult = MiddleResult.collect()
    //    localMiddleResult.foreach(println)
    schemaOfResult = MiddleResult.schema
    //中间结果均需要写到本地，勿写到hdfs
    //    MiddleResult.coalesce(1).write.format("csv").mode(SaveMode.Overwrite).save("file://" + savePath) //需要上传至本地文件
    //写到hdfs，报错
    //    MiddleResult.coalesce(1).write.format("csv").mode(SaveMode.Overwrite).save("hdfs://172.16.32.139:9000/usr/cl")
    //写到本地  savePath\ci\或 savePath/ci/
    val bos = new FileOutputStream(savePath + "ci");
    //    val bos = new FileOutputStream(savePath +System.lineSeparator()+ "ci");
    val oos = new ObjectOutputStream(bos)

    //保存参数的case class
    val params1 = Params(localMiddleResult, schemaOfResult, jsonStr)
    oos.writeObject(params1)
    oos.flush()
    oos.close()
    bos.flush()
    bos.close()
    0
  }

  //predictSet是指用户要传的那列传感器，目前仅支持1列
  //@warn:不继承接口
  //  def predict(predictSet: Array[Row]): Array[Row] = {

  //继承接口
  def predict(predictSetRaw: Array[T with Object]): Array[T with Object] = {

    val predictSet = predictSetRaw.asInstanceOf[Array[Row]]
    //要生成二层网络的传递熵值
    //用户传进某传感器，获取该传感器的列名

    //cl:获取用户要传的传感器，生成全部指向该传感器的“二层网络图”
    //    val  sensorName = predictSet(0).getString(0)

    //目前只能传1列传感器，故取第1列第1行的字段名（其实也可以实现传多列，也就是对多个节点产生“二层网络”）
    val sensorName = predictSet(0).schema.fieldNames(0)
    println("sensorName:" + sensorName + "--------------------------------")

    val rdd_loaded = sparkContext.parallelize(localMiddleResult, parallelism)
    val df_loaded = spark.createDataFrame(rdd_loaded, schemaOfResult)

    println("df_loaded show:  use in python-------------------------------------------")
    df_loaded.show(1000)

    //输出给python展示（未筛选过的图）
    println("pValue:"+pValue+"---------------------------------------------------------")
    println("teValue:"+threshold+"-----------------------------------------------------")
    df_loaded.coalesce(1).write.mode(SaveMode.Overwrite).csv("/usr/cl/transferEntropy/out/MiddleResult")

    //cl:图的顶点
    //此处MiddleResult不是指边，而只是中间结果
    val nodes = df_loaded.select("srcToTarget").collect().map(x => x.getString(0).split("->")).flatMap(x => x).distinct//toSet
    println("nodes: a set!--------------------------------------------")
    nodes.foreach(println)

    // 图的DataFrame形式（包含边三元组的所有信息）
    val linksOfGraph = df_loaded.select("srcToTarget", "teValue", "pValue")
      .withColumn("srcToTarget", split(col("srcToTarget"), "->"))
      .withColumn("src", col("srcToTarget")(0))
      //      生成含srcToTarget,src,target,teValue,pValue这几列的DF
      .withColumn("target", col("srcToTarget")(1))
      //      筛选teValue>te阀值的行
      .where(col("teValue") > threshold)
      //筛选pValue<p阀值的行
      .where(col("pValue") < pValue)
      .where("src != target")         //可能会影响到后面的
      .drop("srcToTarget", "pValue")
    println("links: a dataframe!-------------------------------------")
    linksOfGraph.show(100)
    //    @cl-0612s

    //获取某顶点的“逆向”的邻接列表（DataFrame中指向target节点的所有顶点，可以使用“图”的边反转，再求“反转图”的邻接列表
    //二层关系图同理：从target起，一层原因：“反转图”的邻接列表；二层原因：一层原因节点的”邻接列表”,使用pregel框架，可遍历N层，传入参数为迭代计算次数N
    //@warn 考虑用graphx实现
    val firstLayerDF = linksOfGraph.where(col("target").===(sensorName))
    //获取“一层原因”的节点名称
    val firstLayerNodes = firstLayerDF.select(col("src")).collect.map(x => x.getString(0))

    println("first layer DF----------------------------------------------")
    firstLayerDF.show()
    println("first layer nodes----------------------------------------------")
    firstLayerNodes.foreach(println)


    //isin接收参数为list:Any*， 是可变参数列表（数组也可）
    //    val secondLayerDF = linksOfGraph.where(col("src").isin(firstLayerNodes:_*))       //此处src应该为target
    val secondLayerDF = linksOfGraph.where(col("target").isin(firstLayerNodes: _*))
    println("sencondLayer: a dataframe!-------------------------------")
    secondLayerDF.show(100)
    println("second layer nodes----------------------------------------------")
    val secondLayerNodes = secondLayerDF.select(col("src")).collect.map(x => x.getString(0))
    secondLayerNodes.foreach(println)
    //第一层和第二层unionr
    val allLinks = firstLayerDF.union(secondLayerDF)
      .where("src != target")
    println("allLinks--------------------------")
    allLinks.show()


    //出度为0的src节点——DF中target分组计数，必然全不为0，所有节点去除这些出度非0节点
    //也可以：收集所有src列元素：其中元素的出度必不为0；  收集所有target列元素，其中元素入度必不为0
    val outDegree = linksOfGraph.groupBy("target").count().select("target").collect().map(x => x.getString(0))
    val outDegreeZeroNodes = nodes.filterNot(outDegree.contains)

    println("nodes: outdegree!=0---------------------------")
    outDegreeZeroNodes.foreach(println)

    val inDegreeNotZeroNodes = linksOfGraph.groupBy("src").count().select("src").collect().map(x => x.getString(0))
    inDegreeNotZeroNodes.foreach(println)


    val rootNodesPossible =outDegreeZeroNodes.intersect(inDegreeNotZeroNodes)
    println("nodes: indegree!=0---------------------------")
    //节点为根原因条件之一：入度为0，出度非0
    println("nodes: root reason to be selected---------------------------")
    println(rootNodesPossible.mkString(","))
    //节点为根原因条件之二：root节点 到 target目标节点（指被分析的该节点），有连通路径
    //逻辑暂缺：1.选出与所有目标节点“连通”的根原因；2.算法：计算二者的“所有连通路径”

    //secondLayer.collect()     //貌似必须返回DataFrame，考虑返回经过筛选后的“图”——table形式


    //    图计算部分,nodes:Array[String],links->linksOfGraph:DataFrame["src","target","teValue"]

    //为顶点集nodes的元素添加VertexId:Long
    val vertices = (0L to (nodes.length - 1).toLong).map(x => (x, nodes(x.toInt)))
    val vertexRDD: RDD[(VertexId, String)] = sparkContext.parallelize(vertices)

    //存放键值对：String->Long
    val map1 = new mutable.HashMap[String, Long]()
    vertices.foreach(x => map1.put(x._2, x._1))
    println("打印map1------------------------------------")
    println(map1)
    //存放键值对：Long,String
    val map2 = new mutable.HashMap[Long, String]()
    vertices.foreach(x => map2.put(x._1, x._2))

    val edgeRDD: RDD[Edge[Double]] = linksOfGraph
      .select("src", "target", "teValue")
      .rdd
      .map(x => (x.getString(0), x.getString(1), x.getDouble(2)))
      .map(x => Edge(map1.get(x._1).get, map1.get(x._2).get, x._3))

    val defaultMissing = ("Uknown")
    val graph = Graph(vertexRDD, edgeRDD, defaultMissing)


    //获取“根原因”候选节点集合：入度为0、出度不为0的顶点集——图中入度/出度为0的顶点，不会出现在inDegrees/outDegrees得到的顶点
    val indegreesNotZero = graph.inDegrees.map(x => x._1.toLong)
    val indegreesZero = graph.vertices.map(x => x._1.toLong).subtract(indegreesNotZero)


    //.foreach(println)   //得到indegrees==0的顶点的VertexId
    val outDegreesNotZero = graph.outDegrees.map(x => (x._1.toLong, x._2))
      .filter(x => x._2 > 0) //过滤得到度数量>1的顶点，而不是过滤得到顶点值>0的顶点 ，此处filter不用，默认只收集出度>0的顶点
      .map(x => x._1)
    val rootNodesCandidate = indegreesZero.intersection(outDegreesNotZero) //出度>0且入度=0的顶点VertexId集合，RDD[Long]
      .collect()
    println("入度为0的nodes---------------------------------")
    println(indegreesZero.collect().mkString(","))
    println(indegreesZero.map(x=>map2.get(x).get).collect().mkString(","))
    println("入度不为0的点————————————-----------")
    println(indegreesNotZero.collect().mkString(","))
    println(indegreesNotZero.map(x=>map2.get(x).get).collect().mkString(","))
    println("全部点————————————-----------")
    println(graph.vertices.map(x=>x._1.toLong).collect().mkString(","))
    println(graph.vertices.map(x=>map2.get(x._1.toLong).get).collect().mkString(","))


    println("出度不为0的nodes---------------------------------")
    println(outDegreesNotZero.collect().mkString(","))

    println("graphx : root nodes possible!--------------------------")
    println("long id:"+rootNodesCandidate.mkString(","))
    println("name:"+rootNodesCandidate.map(x=>map2.get(x).get).mkString(","))
    //计算连通分量,有缺陷,不使用
    //    val cc = graph.connectedComponents()
    //    //根节点VertexId若和选定的target节点，所在连通图的minVertexId一致，则保留，作为真正的“根原因”
    //    //连通分量的所有（VertexId,VertexId）——即（graph顶点，graph的连通分量的Id）
    //    val ccVerticesInfo = cc.vertices.map(x=>( x._1.toLong,x._2.toLong)).collect()
    //    val hashMapCC = new mutable.HashMap[Long,Long]()
    //    ccVerticesInfo.map(x=> hashMapCC.put( x._1,x._2))
    //
    //    //得到根原因节点的集合Array[Long]
    //    val rootReasons = rootNodes.filter( x=> map1.get(sensorName).get==hashMapCC.get(x).get )
    //
    //    //根据map2得到根原因节点的集合的Array[String]
    //    val rootReasonsSensorName = rootReasons.map(x=>map2.get(x).get)


    //计算最短路径是否存在（即root节点是否能到达指定的target——ShortestPaths
    val landmarks = Seq(map1.get(sensorName).get)
    val arrsRoots = findShortestPaths(graph, landmarks)

    println("可能根原因中,能到达target节点的根原因集合——————————————————--——————")
    arrsRoots.foreach(println)
    println(arrsRoots.map(x => map2.get(x).get).mkString(","))

    println("目标节点的long id和SensorName-------------------------------------")
    println(sensorName+":"+map1.get(sensorName).get)

    println("root nodes filtered at last----------------------------------")
    val rootNodesAtLast = rootNodesCandidate.intersect(arrsRoots)
    println(rootNodesAtLast.map(x => x+"->"+map2.get(x).get).mkString(","))

    //    图计算部分


    //    allLinks.collect()
    allLinks.collect().asInstanceOf[Array[T with Object]]

  }

  //cl:不继承接口
  //  def train(trainData: Array[Row], inputColIndex: Array[Integer], outputColIndex: Array[Integer]): Integer = {


  //使用Array[T]接口
  def train(trainDataRaw: Array[T with Object], inputColIndex: Array[Integer], outputColIndex: Array[Integer]): Integer = {
    val trainData = trainDataRaw.asInstanceOf[Array[Row]]
    //df根据inputColIndex选取列的另一种方法，假设由Array[Row]生成df——好处在于不需要再用RDD构造，不需要重新定义schema
    //则新df为 ：
    // val newColumns = (0 to inputColIndex.length-1).map( x => col(df.columns(x)))
    // val newDF = df.select(newColumns:_*)

    val schemaString = "srcToTarget k l delay teValue pValue"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)


    //    val lines = sparkContext.textFile((inputFilePath),24)
    //    val rdd = lines.map(x=>x.split(",")(0)+"@"+x.split(",").toBuffer.remove(0).toArray.mkString(","))
    val schema1 = trainData(0).schema
    val schema1Names = schema1.fieldNames

    //模式匹配：如果inputColIndex为空，即长度为0，则选取所有列
    val schemaNames1 = inputColIndex.length match {
      case 0 => schema1Names;
      case _ => inputColIndex.map(x => schema1Names(x)) //选取指定列,dataframe选取这些列作计算
    }

    //获取原schema的字段名->数据类型的映射
    //    val fieldsMap = (0 to schema1.fieldNames.length-1).map(x => (schema1.fieldNames(x),schema1.fields.apply(x) ))
    //    val schemaNames1 = inputColIndex.map(x => schema1Names(x))   //选取指定列,dataframe选取这些列作计算
    //    val trainData1 = inputColIndex.map(x=>)
    val rdd0 = sparkContext.parallelize(trainData)
    val cols = schemaNames1.map(x => col(x))

    val fieldsNew = (0 to schemaNames1.length - 1).map(x => schema1.fields.apply(x))
    val schemaNew = StructType(fieldsNew)
    //此处应该使用新的schema,而不是schema1,否则train和predict阶段的字段名会不一致
    //    val trainData1 = spark.createDataFrame(rdd0,schema1)
    val trainData0 = spark.createDataFrame(rdd0, schemaNew)
      .select(cols: _*) //
    //                         .collect()
    //                          .drop(1)      //Array中丢弃某个元素，从1开始，而非从0开始
    //cl:Dataframe转置

    println("trainData0--------------------------------")
    trainData0.show()
    val trainData1 = trainData0.collect().drop(1)
    //已经是转置了，Dataframe转置的过程——此处已实现，也可看DenseMatrix的transpose方法
    //Matrix构造必须是Array[Double],未采用其转置
    val arrs = {
      0 to schemaNames1.length - 1
    }.map(x => trainData1.map(y => y.getString(x)).mkString(",")).toArray
    var arrs1 = {
      0 to schemaNames1.length - 1
    }.map(x => schemaNames1(x) + "," + arrs(x)).toArray
    //scala上没错
    println("arr1 value!")
    //    arrs1.foreach(println)

    //                        .map(x=>x.mkString(","))
    //
    //    val m_inputCnt = inputColIndex.size
    //    val m_inputCols = new ArrayBuffer[String]()
    ////    val rdd0 = sparkContext.parallelize(trainData)
    //    for(i<-0 until inputColIndex.size){
    //      m_inputCols.append(schema.fieldNames(inputColIndex(i)))
    //    }
    //    val sc = new SQLContext(m_sparkContext)


    val trainSet = arrs1
    //    val trainSet = trainData.map(_.mkString(","))

    val rdd = sparkContext.parallelize(trainSet, parallelism)
    rdd.cache()
    println("train_point0")
    val rdd1 = rdd.map(x => x.replaceFirst(",", "@"))

    //此处广播变量不太适用
    //    val cartesian_array = sparkContext.broadcast(rdd1.cartesian(rdd1).repartition(parallelism).collect())

    val lines_cartesian = rdd1.cartesian(rdd1).repartition(parallelism)

    //持久化策略
    lines_cartesian.persist(StorageLevel.MEMORY_AND_DISK)

    println("train_point1")
    //    lines_cartesian.take(20).foreach(println)
    //cl:时序定阶
    //warn:这里耗时过长，2W3*16的数据，此处耗时5-7min，貌似是spark-ts库的并发锁导致，也有可能是collectAsMap导致，考虑使用别的方式collect
    //时序定阶————注意Vector是scala自带的类型，Vector则是spark中自定义的类（非类型),且vector类所在包，有静态类Vectors，勿混淆
    val map_ts_k = rdd1.map(x => (x.split("@")(0), findBestArmaP(Vectors.dense(x.split("@")(1).map(_.toDouble).toArray), maxP, 0, 0, k_max))
      //这一句定阶：如果ARIMA中差分也无法平稳，则默认为K_MAX，只计算K_MAX的值
      //      ( x.split("@")(0),findBestArmaP(Vectors.dense(x.split("@")(1).map(_.toDouble).toArray),maxP,maxD,maxQ,k_max   )         )      //findBestArmaP( DenseVector,maxP,maxD,maxQ )
      //由于ARIMA考虑到时序的平稳性，若在maxD内都不平稳，会报错。为了定阶，AR就够了，可以考虑maxD和maxQ定阶为0
      //k_max为防止上面ARIMA报错，给个默认的k值输出
      //      ( x.split("@")(0),findBestArmaP(Vectors.dense(x.split("@")(1).map(_.toDouble).toArray),maxP,0,0,k_max   )         )
    ).collectAsMap()

    //    println(map_ts_k.get("AI7006").get)
    println("train_point2")
    val broadcastMap = sparkContext.broadcast(map_ts_k) //广播变量，缓存srcName->best_k的Map

    //    val hashMap = new mutable.HashMap[String,Int]()
    //    map_ts_k.map(x => hashMap.put(x._1,x._2))

    print("stage hashMap done!")

    //测试
    //    lines_cartesian.take(5).foreach(println)

    //cl:计算传递熵

    //计算传递熵矩阵（未定阶）
    val rowRDD = lines_cartesian
      //            生成 4元元组 （srcName, srcSeries,destName,destSeries) ,其中，srcName,destName为String类型，表示source和target的变量名
      //              srcSeries和destSeries是Array[Double]类型， 作为传递熵值计算的输入，两个 double[]类型的序列
      .map(x => (x._1.split("@")(0), x._1.split("@")(1).split(",").map(_.toDouble).toArray, x._2.split("@")(0), x._2.split("@")(1).split(",").map(_.toDouble).toArray))
      //      .map( x => (x._1+"->"+x._3 , computeTwoSeriesTEsStringResultGaussianSrcName (x._1,hashMap,x._2,x._4 ,3,k_max,k_tau,l_max,l_tau,delay_max) ) )
      //将上面的hashMap:HashMap 修改为hashMap:Map类型

      .map(x => (x._1 + "->" + x._3, computeTwoSeriesTEsStringResultGaussianSrcName(x._1, map_ts_k, x._2, x._4, 3, k_max, k_tau, l_max, l_tau, delay_max)))
      //      .map(x => (x._1 + "->" + x._3, computeTwoSeriesTEsStringResultGaussianSrcName(x._1, broadcastMap.value, x._2, x._4, 3, k_max, k_tau, l_max, l_tau, delay_max)))
      .flatMap(y => y._2.map(result => (y._1, result)))
      //            .flatMap(y => y._2.map(b=> y._1+","+b )    )
      .map(x => Array(x._1, x._2).mkString(",")) //此处全部用“，”分隔，后面构造dataframe需要

      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4), attributes(5)))

    //    rowRDD.foreach(x=>x.getString(0))

    // Apply the schema to the RDD
    var DF = spark.createDataFrame(rowRDD, schema)
    //除srcToTarget列外，全转成Double
    for (column <- DF.columns if !column.equalsIgnoreCase("srcToTarget")) {
      DF = DF.withColumn(column, col(column).cast(DoubleType))
    }



    //cl:每两个时间序列X->Y ，求出Max( Te(X->Y) )

    //分组取最大值——即分组求top1
    val w = Window.partitionBy("srcToTarget").orderBy(col("teValue").desc)
    //    降序排序
    MiddleResult = DF.withColumn("rn", row_number().over(w)).where(col("rn") === 1).drop("rn") //计算边的邻接矩阵
      .where(col("pValue") < pValue)

    println("show middle result-------------------------------")
    MiddleResult.show(1000)
    0
  }

}