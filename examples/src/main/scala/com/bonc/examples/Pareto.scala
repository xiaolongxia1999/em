package com.bonc.examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
/**
  * Created by Administrator on 2018/9/25 0025.
  */
//class Mopso {
//
//}
//
//
//class Pareto(fitIn:DataFrame) {
//}

object Pareto {
  def main(args: Array[String]): Unit = {
    val dataPath = "D:\\pycharm\\PycharmProjects\\MOPSO\\data\\dataset.csv"
    val jsonPath = ""

    val conf = new SparkConf().setMaster("local[*]").setAppName("Pareto")
    val spark = SparkSession.builder().config(conf).getOrCreate()

//    val options = Map{"header"->true}
    var df = spark.read.option("header", true).option("inferSchema", true).csv(dataPath)
//    df.show()
    df.printSchema()
    df.na.fill(0)

    df = df.withColumn("particles_id", row_number().over(Window.orderBy(df.columns(0))))
    df.show()
  }
}