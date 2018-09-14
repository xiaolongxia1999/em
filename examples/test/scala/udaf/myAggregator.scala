package udaf

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

case class Employee1(name: String, salary: Long)
case class Average(var sum: Long, var count: Long)
/**
  * Created by Administrator on 2018/9/6 0006.
  */
class myAggregator extends Aggregator[Employee1, Average, Double]{
  def zero: Average = Average(0L, 0L)

  def reduce(b: Average, a: Employee1): Average = {
    b.sum += a.salary
    b.count += 1
    b
  }

  def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  def finish(reduction: Average): Double = {
    reduction.sum.toDouble / reduction.count.toDouble
  }

  def bufferEncoder: Encoder[Average] = Encoders.product

  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

object myAggregator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("avg").getOrCreate()

    import spark.implicits._

    val df = Seq(
      Employee1("Michael", 3000),
      Employee1("Andy", 4500),
      Employee1("Andy", 3500),
      Employee1("Berta", 4000)
    ).toDS().toDF("name1","salary1")

    df.show()

    val obj = new myAggregator()
    val col1 = obj.toColumn(col("salary1"))
    val df1 = df.select(col1)
    df1.show()
  }
}