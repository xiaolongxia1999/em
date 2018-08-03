package com.bonc.utils

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.sql.DataFrame

/**
  * Created by yongchaowei on 2018/7/30.
  */
object FAutils {

  def GBDTFS(df:DataFrame,labelCol:String,contrib_rate1:Double,is_filter:Int):Array[(String,Double)]={
    val inputCols=df.columns.filter(_!=labelCol)
    val vectorAssembler = new VectorAssembler()
      .setInputCols(inputCols)
      .setOutputCol("featureVector")

    val GBDTRegressor = new GBTRegressor()
      .setFeaturesCol("featureVector")
      .setLabelCol(labelCol)
      .setPredictionCol("prediction")
      .setMinInfoGain(0.0)
      .setMaxDepth(3)
      .setMaxIter(10)
      .setMaxBins(20)

    val pipeLine = new Pipeline().setStages(Array(vectorAssembler,GBDTRegressor))
    val plModel = pipeLine.fit(df)
    //val prediction = plModel.transform(df)
    val GBDTRegressionModel = plModel.stages.last.asInstanceOf[GBTRegressionModel]
    val sortedFeatureImportanceArray = GBDTRegressionModel.featureImportances.toArray.zip(inputCols).sorted.reverse
    //val filteredCols1 = sortedFeatureImportanceArray.filter(_._1>threshold).map(x=>x._2)

    //梯度提升指标筛选
    if (is_filter == 1) {
      var contribution_Rate = 0.0
      var i = 1
      while (contribution_Rate <= contrib_rate1) {
        contribution_Rate = contribution_Rate + sortedFeatureImportanceArray.take(i).last._1
        i += 1
      }
      //val filteredCols = sortedFeatureImportanceArray.take(i-1).map(x=>x._2)
      sortedFeatureImportanceArray.take(i - 1).map(x => (x._2, x._1))
      //val dropCols=inputCols.filterNot(x=>filteredCols.contains(x))
      // val filteredDF = df.drop(dropCols:_*)
      //return filteredDF
    }else{
      sortedFeatureImportanceArray.map(x=>(x._2,x._1))
    }
  }
}
