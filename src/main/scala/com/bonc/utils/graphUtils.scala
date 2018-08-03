package com.bonc.utils

import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

/**
  * Created by Administrator on 2018/7/2 0002.
  */
object graphUtils {

  def findShortestPaths(graph: Graph[String,Double],landmarks:Seq[Long]):Array[Long]={
    val shortestPathsGraph = ShortestPaths.run(graph,landmarks)
      .vertices
      //保留寻找最小路径后，非空的Map且不等于landmark节点里的
      .filter(x=> x._2.nonEmpty && !landmarks.contains(x._1))
      .collect()
      .map(x=>x._1.toLong)

    shortestPathsGraph
  }

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("graphxExamples"))
    val spark = SparkSession.builder().master("local[*]").appName("graphxExamples").getOrCreate()
    val nodes = Array("s1","s2","s3","s4");
    //注意里面装的是一些边
    val links = Array(
      Edge(1L,2L,0.1),
      Edge(1L,3L,0.1),
      Edge(2L,4L,0.1),
      Edge(2L,3L,0.1),
      Edge(5L,4L,0.1),
      Edge(6L,4L,0.1)
    )

    val pair = (0L to (nodes.length-1).toLong).map(x=> (x,nodes(x.toInt)) )
    pair.foreach(println)



    val vertexRDD:RDD[(VertexId,String)] = sc.parallelize(pair)
    val edgesRDD = sc.parallelize(links)
//    val graph = Graph(vertexRDD,edgesRDD,(0.2))
    val graph = Graph.fromEdges(edgesRDD,(0.2))

    graph.outDegrees.foreach(println)

    println("graph only")
    println("graph edges-----------------------")
    graph.edges.foreach(println)
    println("graph vertices-----------------------")
    graph.vertices.foreach(println)
    println("connectedComponents-------------------")
    println("connectedComponents edges-------------")
    graph.connectedComponents.edges.foreach(println)
    println("connectedComponents vertices-----------")
    graph.connectedComponents.vertices.foreach(println)
//打印结果如下：其中一个图可以计算得到多个连通图， 比如上例有2个连通图，顶点（1,2,3）,(4,5,6)分别在2个不同的连通图中
//    而连通图的标识，使用该连通图的顶点集中最小的顶点的VertexId作为标识
//    通过connectedComponents.vertices可以得到  顶点是属于哪个连通图

//    简言之：连通图的标识，通过生成连通图后的Vertices可以查看，是一个二元组 （顶点Id, 所在连通图最小顶点的Id）
//    connectedComponents
//    Edge(1,3,0.1)
//    Edge(6,4,0.1)
//    Edge(2,1,0.1)
//    Edge(5,4,0.1)
//    Edge(2,3,0.1)
//    Edge(1,2,0.1)
//    vertices
//    (3,1)
//    (5,4)
//    (6,4)
//    (1,1)
//    (2,1)
//    (4,4)
//    graph


    //计算根原因：


    val connectedComponentsInfo = graph.connectedComponents().vertices.collect().map(x=>x._1)

//
//    println("indegrees and outdegrees")
//    println("outdegrees>0")
//    graph.outDegrees
//      .map(x=>(x._1.toLong,x._2)).filter(x=>x._2>0).foreach(println)
//    println("indegrees==0")
//    //默认计算出度和入度都是>0的，出入度=0的顶点不在出入度顶点内
//    graph.inDegrees
//      .map(x=>(x._1.toLong,x._2))
////      .filter(x=>x._2==0)
//      .foreach(println)
//    println("indegrees == 0  and outdegrees >0 ")
////    graph.outDegrees
////      .map(x=>(x._1.toLong,x._2)).filter(x=>x._2>0)     //转成新RDD
////      .intersection(
////      graph.inDegrees.map(x=>(x._1.toLong,x._2)).filter(x=>x._2>0)
////
////    )
////      .foreach(println)

    val indgreesNotZero = graph.inDegrees.map(x=>x._1.toLong)
    val indegreesZero = graph.vertices.map(x=>x._1.toLong).subtract(indgreesNotZero)//.foreach(println)   //得到indegrees==0的顶点的VertexId

    val outDegreesNotZero = graph.outDegrees.map(x=>(x._1.toLong,x._2))
    .filter(x=>x._2>1).map(x=>x._1)         //过滤得到度数量>1的顶点，而不是过滤得到顶点值>1的顶点
//    .foreach(println)


    indegreesZero.intersection(outDegreesNotZero).foreach(println)







    //顶点用String类型标识时，使用HashMap先生成（K.V)对， VertexId->StringValue,使用hashMap.



  }
}
