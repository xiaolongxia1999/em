package com.bonc.Interface

import org.apache.spark.sql.Row

/**
  * Created by yongchaowei on 2018/6/4.
  */
trait ICalc1 {

  def calc(inputData: Array[Row]): Array[Row] = {Array(Row("default"))}


  def setParam(paramStr: String) : Integer = {1}


}
