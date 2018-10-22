

import com.bonc.models.Pareto
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import util.control.Breaks._
/**
  * Created by Administrator on 2018/9/26 0026.
  */
object Pareto1 {
  def compare(currRow:Row, refRow:Row):Boolean = {
    //      var flag:Boolean = true
    val size = currRow.length
    for(index<-0 to size - 1){
//      if (currRow.getDouble(index) < refRow.getDouble(index)) {
//        flag = true
//      } else {
//        flag = false
      if (currRow.getDouble(index) < refRow.getDouble(index)){
        true
      }
    }
    false
  }

  //    scala中break和continue:https://blog.csdn.net/u010454030/article/details/53931665
  def judge(currRow:Row, localRows:Array[Row]):Boolean={

    //      var flag:Boolean = true
    //    加入守卫条件
    for(index<-0 to localRows.length - 1 if index != localRows.indexOf(currRow)) {
      if(compare(currRow, localRows(index)) == false) {
        false
      }
    }
    true
  }

  def paretoLocal(local:Iterator[Row]):Iterator[Row]={
    var localArray = local.toArray
    val size = localArray.length
    //      val bad_num = 0L
    //      var cursor = -1L
    //      def next():Long = cursor + 1
    //      def hasNext():Boolean = size >  cursor + 1 + bad_num

    for(index<-0 to size - 1 ) {
      if (judge(localArray(index), localArray ) == false) {
        //          去掉被占优的当前行
        localArray = localArray.drop(index)
      }
    }
    localArray.toIterator
  }


  def main(args: Array[String]): Unit = {

//    @warn 不要用这个作为Pareto比较，4分钟，用4个目标的fitness矩阵
//    val data_path = "E:\\IdeaWorkSpace\\EnergyManagement4\\EnergyManagement\\python\\python\\MOPSO\\data\\dataset.csv"
    val data_path = "E:\\bonc\\工业第四期需求\\数据\\out\\16决策和4目标变量0927.csv"

    val conf = new SparkConf().setAppName("pareto").setMaster("local[1]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    var df = spark.read.option("header",true).option("inferSchema",true).format("csv").load(data_path)
    df.show()

    val df1 = df.select("goal1","goal2","goal3","goal4")
    df1.show()
//    df.printSchema()

//    df = df.withColumn("id", row_number().over(Window.orderBy(df.columns(0))))

    var rdd = df1.rdd
    val schema = df1.schema
//    val localRows = rdd.collect().drop(1)
//    val resultRows = paretoLocal(localRows.toIterator)
//    val newRDD = spark.sparkContext.parallelize(resultRows)
//    val df2 = spark.createDataFrame(newRDD, schema)
//    df2.show()
//    println(df2.count())
//    val rdd1 = rdd.aggregate()

    val rdd1 = rdd.mapPartitions[Row]( iter =>{
      val obj = new Pareto(iter.toArray,0)
      obj.pareto().toIterator
    })



//      本地版测试
//    val obj = new Pareto(rdd.collect())
//    val newArr = obj.pareto()
//    val rdd1 = spark.sparkContext.parallelize(newArr)
//
//    println(s"rdd1.length is  ${rdd1.count()}")


//      .coalesce(1)
//      第二次比较时，集中到一个分区，统一作占优比较

    val df2 = spark.createDataFrame(rdd1, schema)
    println(s"df2 length is: ${df2.count()}")
    df2.show()

//    val rdd1 = rdd.foreachPartition(iter=>
//
//    )

  }

}

