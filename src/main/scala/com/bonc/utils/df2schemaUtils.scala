package com.bonc.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, IntegerType}

/**
  * Created by yongchaowei on 2018/7/31.
  */
object df2schemaUtils {
  def df2schema(df1:DataFrame):DataFrame={
    var df = df1
    for(colname <- df.columns ){
      if (colname !="time") {
        df = df.withColumn(colname, df.col(colname).cast(DoubleType))
      }else{
        df = df.withColumn("time",df.col("time").cast(IntegerType))
      }
    }
    df
  }

}
