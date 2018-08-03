package com.bonc.examples

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}

/* Created by Administrator on 2016/1/13
*/
object test {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)


//    val conf = new SparkConf().setMaster("local[*]").setAppName("WorkingCondition").set("spark.driver.memory","2g").set("spark.executor.memory","2g")
//    val spark = SparkSession.builder().config(conf).getOrCreate()
//    val sc = spark.sparkContext
////@warn
//    val inputfile ="E:\\bonc\\工业第三期需求\\bonc第3期项目打包\\能源管理平台3期0720\\能源管理平台3期\\相似工况生产控制场景\\data\\相似工况真实场景_29.csv"
//    val inputfile ="C:\\Users\\魏永朝\\Desktop\\相似工况真实场景_29.csv"


    //分布式下
//    val inputfile = args(0)
//    val json_path =args(1)
//    val saveModelPath = args(2)
    val appName = "WorkingCondition"
    val master = "local[*]"
    val conf = new SparkConf().setAppName(appName).set("spark.serializer","org.apache.spark.serializer.KryoSerializer").setMaster(master)
    @transient
    val sparkContext = new SparkContext(conf)
    @transient
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    val dir = "C:\\Users\\魏永朝\\Desktop\\能源管理平台3期\\异常侦测应用场景\\"
    val path = dir+ "data\\ProcessedAbnormalDetection.csv"

    var df1 = spark.read.option("header",true).csv(path)
    println(df1.schema)

    for(colname <- df1.columns ){
      if (colname !="time") {
        df1 = df1.withColumn(colname, df1.col(colname).cast(DoubleType))
      }else{
        df1 = df1.withColumn("time",df1.col("time").cast(IntegerType))
      }
    }

    df1.show()

    println(df1.schema)


    if(1==2) {
      df1.cache()
      val df = df1.na.fill(0.0).filter(!_.toSeq.contains(0.0))
      df.cache()
      val obsvercol = Array("GT2发电机功率", "GT1发电机功率", "汽机功率")
      println(df1.columns.length)
      println(df1.columns.diff(obsvercol).length)
      val selectcol = df1.columns.diff(obsvercol).map(x => col(x))


      df1.select(selectcol: _*)
      df1.show()
      df1.drop(obsvercol(2)).show()

      val testData = spark.createDataFrame(Seq(
        (2010, 1),
        (1891, 0),
        (1892, 1),
        (1893, 1),
        (1894, 1),
        (2011, 1),
        (1896, 0),
        (1897, 0),
        (1898, 1)
        , (1899, 1),
        (1900, 0),
        (1901, 1)
        , (1902, 1),
        (1903, 1),
        (1904, 1)
        , (1905, 0),
        (1906, 0),
        (1907, 0)
        , (1908, 0),
        (1909, 1),
        (1910, 1),
        (1911, 0),
        (1912, 0),
        (1913, 1),
        (1914, 1),
        (1915, 0),
        (1916, 0)
      )).toDF("time", "type")

      testData.sort("time").show(50)

    }
//    val h = Array(  Array("1.2","2.3","4.3","7.8","9.8"),Array("3.9","7.1","7.9"),Array("1.8","2.8"))
//    h.reduce( (x,y)=> x.flatMap(e=>y.map(f=>e+f))  )
//    var ddd= h.reduce( (x,y)=> x.flatMap(e=>y.map(f=>e+","+f))  )
//    var mmm= ddd.map(x=>x.split(",").map(_.toDouble))
//    mmm.toList.toDF().show(false)
//
//    val ff = Array(3,2,4,2,5)
//
//    val ffmm= ff.indexOf(ff.min)
//    println(ffmm)
//
//
//
//
//
//    var df =spark.read.option("header",true).option("inferSchema",true).csv("C:\\Users\\魏永朝\\Desktop\\工况划分测试.csv")
//    df.cache()
//
//    var trainData = df.limit(1)
//    val labelCol =  "气耗率"
//    val va = new VectorAssembler()
//      .setInputCols(trainData.columns.filter(_!=labelCol))
//      .setOutputCol("featureVector")
//
//    val sa = new StandardScaler()
//      .setInputCol("featureVector")
//      .setOutputCol("scaledVector")
//
//    val lr = new LinearRegression()
//      .setFeaturesCol("scaledVector")
//      .setLabelCol(labelCol)
//      .setPredictionCol("prediction")
//    //.setElasticNetParam(0.3)
//    //.setRegParam(0.5)
//    //.setMaxIter(10)
//
//    val pl = new Pipeline().setStages(Array(va,sa,lr))
//    val plModel = pl.fit(trainData)
//
//    //df.show()
//    //df.printSchema()
//
//    //设置工况条件以及其偏差
//    //val boundaryCol = Array(("GT2发电机功率",5.0),("GT1发电机功率",2.0),("汽机功率",3.0))
//    val boundaryCol = Array(("GT2发电机功率",60.0),("GT1发电机功率",70.0),("功率总和",100.0),("汽机功率",60.0))
//
//    workcondtion(boundaryCol,df).foreach(println)



   // val boundaryCol = bj.zip(basis)


    //val startValue = Array()
    //val cll = ArrayBuffer[Array[Double]]()
   // var dffff = spark.emptyDataFrame
//    var m_rdd =spark.emptyDataFrame
//    var duo_array = new Array[Array[String]](boundaryCol.length)
//
//    for(i<-boundaryCol.indices){
//      println(i)
//      println(boundaryCol(i)._1)
//      var minValue = df.agg(min(boundaryCol(i)._1)).head().getDouble(0)-1e-8
//      //println(minValue)
//      var maxValue = df.agg(max(boundaryCol(i)._1)).head().getDouble(0)+1e-8
//      //println(maxValue)
//      var centro = 0.0
//      var basis_1 = boundaryCol(i)._2/2.0
//      //====================================
//      //val cll = ArrayBuffer[Array[Double]]()
//      val cl = ArrayBuffer[String]()  //空的可变数组
//
//
//      while(minValue<maxValue){
//        centro = minValue+basis_1
//        cl.append(centro.toString)
//        minValue+=basis_1*2.0
//      }
//      duo_array(i)=cl.toArray
//      if (i==0){
//        m_rdd = cl.toList.toDF("类别${i}")
//       // m_rdd.show()
//      }else{
//        println("i="+i)
//        var a_m = ArrayBuffer[Double]()
//        m_rdd =m_rdd.rdd.cartesian(cl.toList.toDF().rdd).collect().map(x=> {
//          var sd= ArrayBuffer[Double]()
//          for(j<- 0 to 0){sd.append(x._1.getDouble(j))}
//          (sd.append(x._2.getDouble(0)))
//          sd.toSeq
//            for (j <- 0 to (i-1)
//            ){
//              a_m.append(x._1.getDouble(j))
//              //println(x._1.getDouble(j))
//            }
//            a_m.append(x._2.getDouble(0))
//          println(s"======================${i}")
//            a_m.foreach(println)
//          println(s"======================${i}")
//            a_m.toSeq
//          }
//          ).toList.toDF()
//        a_m.clear()
        //ff.toList.toDF().show()





      }


//      var cl_rdd = sc.makeRDD(cl)
//      if(m_rdd.isEmpty()){
//        m_rdd = cl_rdd
//      }else{
//        m_rdd= m_rdd.cartesian(cl_rdd)
//      }
      //var  dfm = cl.toList.toDF(s"类别${i}")
      //dfm.show()
//      println(dffff.count())
//      if(dffff.count() ==0){
//        dffff = dfm
//        println("-------"+dffff.count())
//      }else{
//        dffff = dffff.join(dfm)
//        dffff.show()


    //val h = Array(  Array("1.2","2.3","4.3","7.8","9.8"),Array("3.9","7.1","7.9"),Array("1.8","2.8"))
   // h.reduce( (x,y)=> x.flatMap(e=>y.map(f=>e+f))  )
//    var dddd= duo_array.reduce((x,y)=> x.flatMap(e=>y.map(f=>e+","+f))).map(x=>x.split(",").map(_.toDouble)).map(Vectors.dense(_))


//    var centroids=Array.empty[Vector]


//    centroids = dddd


//    println("=========================================================================")
//    var df1=duo_array(0).toList.toDF("12")
//    var df2 = duo_array(1).toList.toDF("22")
//    var df3 = duo_array(2).toList.toDF("33")
//    var df4 = duo_array(3).toList.toDF("44")


//    var nn = df1.rdd
//      .cartesian(df2.rdd).map(x=>(x._1.getDouble(0),x._2.getDouble(0))).collect().toList.toDF().rdd
//      .cartesian(df3.rdd).map(x=>{var ss = ArrayBuffer[Double]() ;for(j<- 0 to 1){ss.append(x._1.getDouble(j))};ss.append(x._2.getDouble(0));ss.toList}).collect().toList.toDF()//.rdd
//      //.cartesian(df4.rdd).map(x=>(x._1.getDouble(0),x._1.getDouble(1),x._1.getDouble(2),x._2.getDouble(0))).collect().toList.toDF()


//    val tokenizer = new Tokenizer().setInputCol("value").setOutputCol("numbers")
//    tokenizer.transform(nn.map(x=>x.toString().replaceAll("\\[","").replaceAll("\\]","").replaceAll("\\,"," "))).show(false)
//    val assembler = new VectorAssembler()
//      .setInputCols(nn.columns)
//      .setOutputCol("features")
//   var jj=  assembler.transform(nn).select("features").show()




    //m_rdd.sort("_1","_2")show(10000,false)





//    var sss = sc.parallelize(duo_array)
//    sss =


//    }
/*
  def workcondtion(boundaryCol:Array[(String,Double)],df:DataFrame):Array[Vector] ={

    var duo_array = new Array[Array[String]](boundaryCol.length)
    for(i<-boundaryCol.indices){
      var minValue = df.agg(min(boundaryCol(i)._1)).head().getDouble(0)-1e-8
      var maxValue = df.agg(max(boundaryCol(i)._1)).head().getDouble(0)+1e-8

      var centro = 0.0
      var basis_1 = boundaryCol(i)._2/2.0
      //====================================
      val cl = ArrayBuffer[String]()  //空的可变数组
      while(minValue<maxValue){
        centro = minValue+basis_1
        cl.append(centro.toString)
        minValue+=basis_1*2.0
      }
      duo_array(i)=cl.toArray
    }
    duo_array.reduce((x,y)=> x.flatMap(e=>y.map(f=>e+","+f))).map(x=>x.split(",").map(_.toDouble)).map(Vectors.dense(_))
  }*/
}

