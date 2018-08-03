package com.bonc.Interface

import org.apache.spark.sql.Row

/**
  * Created by patrick on 2018/4/12.
  */
trait Interface {
  def train(inputData: Array[Row], inputColIndex: Array[Integer], labelColIndex: Integer): Integer ={println("original train fun in trait");1}

  def predict(inputData: Array[Row]): Array[Row] = {Array(Row("default"))}

  def saveModel(modelFilePath: String): Integer = {1}

  def loadModel(modelFilePath: String): Integer = {1}

  def setParam(paramStr: String) : Integer = {1}

  def getRMSE(inputData: Array[Row], labelColIndex: Integer): Integer = {println("this is in test trait");1}
}
