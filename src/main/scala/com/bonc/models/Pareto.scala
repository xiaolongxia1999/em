package com.bonc.models

import java.io.{File, PrintWriter}

import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import util.control.Breaks._

/**
  * Created by Administrator on 2018/9/26 0026.
  */

//目标分量越多，时间越长，得到的可能跟原数据差不多，所以使用原始输入的data.csv, 16个分量，结果Pareto结果是原数据本身
class Pareto(arrayRow:Array[Row],fitColFrom:Int) extends  Serializable {
//    fitColFrom 是各目标函数的起始列，也等于决策变量的长度
  var localRows = arrayRow
  var cursor = -1
  val len = arrayRow.length
  var badNum = 0
  var colSize = arrayRow(0).length

  def next(): Row = {
    cursor = cursor + 1
    localRows(cursor)
  }

  def hasNext(): Boolean = {
    len > cursor + 1 + badNum
  }

  def remove(): Unit = {
    val droped = cursor
//    注意scala的Array的drop(n)，默认是去除左边前n个元素，而不是第n各元素——这里困扰了我两天，终于改正确
//    Array和ArrayBuffer删除元素，详见：https://blog.csdn.net/Veechange/article/details/53713050?locationNum=10&fps=1
    val buffer = localRows.toBuffer
    buffer.remove(droped)
    localRows = buffer.toArray
    cursor = cursor - 1
    badNum = badNum + 1
  }

  def pareto(): Array[Row] = {
    while (hasNext()) {
      val currRow = next()
      //截取Fitness列部分，作Pareto比较
      val currFitRow = Row(currRow.toSeq.slice( fitColFrom, colSize).toSeq:_*)
      val localFitRows = localRows.map(row => Row(row.toSeq.slice( fitColFrom, colSize).toSeq:_*))

      if (Pareto.judge(currFitRow, localFitRows, cursor) == false) {
//      if (Pareto.judge(currRow, localRows, cursor) == false) {
        remove()
      }
    }
    localRows

    //  def paretoForPartition(rdd:RDD[Row]):RDD[Row] = {
    //    rdd.mapPartitions(iter => {
    //      val localRow = iter.toArray
    //      this.pareto().toIterator
    //    })
    //  }
  }
}

object Pareto {
    //  两行相比，前者全>后者，则为false，否则为true
    def compare(currRow: Row, refRow: Row): Boolean = {
      //      var flag:Boolean = true
      val size = currRow.length
      for (index <- 0 to size - 1) {
        if (currRow.getDouble(index) < refRow.getDouble(index)) {
          return true
        }
      }
      return false
    }


    //    scala中break和continue:https://blog.csdn.net/u010454030/article/details/53931665
    def judge(currRow: Row, localRows: Array[Row], cursor: Long): Boolean = {
      //    var flag:Boolean = true
      //    加入守卫条件
      for (index <- 0 to localRows.length - 1) {
        breakable(
          if (index == cursor) {
            break()
          } else if (compare(currRow, localRows(index)) == false) {
            return false
          } else {
            break()
          }
        )
      }
      return true
    }


    def main(args: Array[String]): Unit = {
//      val input0 = Array(
//        Array(1, 3, 4, 1),
//        Array(2, 3, 4, 1), //s
//        Array(1, 3, 1, 2),
//        Array(2, 3, 4, 3), //s
//        Array(5, 3, 4, 2), //s
//        Array(2, 3, 4, 5)  //s
//      )
//      val input = input0.map(x => x.map(_.toDouble))
//      val arr = input.map(x => Row.fromSeq(x.toSeq))
//      val pareto = new Pareto(arr)
//      val result = pareto.pareto()
      val sparkConf = new SparkConf().setAppName("pareto").setMaster("local[*]")
      val spark = SparkSession.builder().config(sparkConf).getOrCreate()
      //加载配置
      val config = new Config()
      config.setDataPath("D:\\pycharm\\PycharmProjects\\MOPSO\\data\\dataset.csv")
              .setJsonPath("D:\\pycharm\\PycharmProjects\\MOPSO\\conf\\fitness_list.json")
//      println(config.jsonPath)
      config.process()
      var df1 = spark.createDataFrame(spark.sparkContext.parallelize(config.data), config.schema)
      //各列转为Double
      for(column<-df1.columns) {
        df1 = df1.withColumn(column, col(column).cast(DoubleType))
      }
      df1.printSchema()
      df1.show
      //计算Pareto
      val df2 = Utils.paretoDataFrame(df1,spark)
      df2.show()
      println(df2.count())
    }

}

//scala二次构造
//https://blog.csdn.net/hellojoy/article/details/81183490
class Mopso (particles:Double,
             w: Double,
             c1: Double,
             c2: Double,
             max : Array[Double],
             min: Array[Double],
             thresh:Long,
             meshDiv: Long,
             config: Config,
             fitGoodIn: Array[Array[Double]],
             fitnessGoodIn: Array[Array[Double]]) {

    var maxV = (0 to max.length - 1).toArray.map(x => (max(x) - min(x)) * 0.05)
    var minV = (0 to max.length - 1).toArray.map(x => (max(x) - min(x)) * 0.05 * (-1))
    var in:Array[Array[Double]] = _
    var fitness:Array[Array[Double]] = _
    var v:Array[Array[Double]] = _
    var Pbest:(Array[Array[Double]],Array[Array[Double]]) = _
    var Archiving:(Array[Array[Double]],Array[Array[Double]]) = _
    var Gbest:(Array[Array[Double]],Array[Array[Double]]) = _



//  每次更新计算适应度
    def evaluateFitness():Unit = {
//      val fieldSet = this.config.schema.fieldNames
//      val exprList = this.config.exprList
//      val in = this.config.data
////      @warn 先把基础模块写好！
//      val fitness = ???
        fitness = Fitness.calcFitness(in, config.exprList, config.inputColumns)
    }


    def initialize():Unit = {
        val dvSize = fitGoodIn(0).length
        val fitSize = fitnessGoodIn(0).length
        in = fitGoodIn
        v = init.initV(particles, maxV, minV)
        evaluateFitness()
        Pbest = init.initPbest(in, fitness)
        //初始化外部储备集
        val inputRows = Utils.stackRows[Double](in, fitness, 0)
        Archiving = init.initArchiving(inputRows, dvSize)
        Gbest = init.initGbest(Archiving._1, Archiving._2,meshDiv,min, max, particles)
    }

    def updateOnce():Unit = {
        v = update.updateV(v, minV, maxV, in, Pbest._1, Gbest._1, w, c1, c2 )
        in = update.updateIn(in, fitness, Pbest._1, Pbest._2)
        evaluateFitness()
        Pbest = update.updatePbest(in, fitness, Pbest._1, Pbest._2)
        Archiving = update.updateArchiving(in, fitness, Archiving._1, Archiving._2, thresh, meshDiv, min, max, particles)
        Gbest = update.updateGbest(Archiving._1, Archiving._2, meshDiv, min, max, particles)
    }

    def process(maxIter:Int):(Array[Array[Double]],Array[Array[Double]]) = {
        initialize()
        for(iterNum<- 0 to maxIter - 1) {
            updateOnce()
        }
        Archiving
    }
}


class Predict(var newRecordsIn:Array[Array[Double]],
              var conf:Config,
              var historyModelPath:String,
              var frontPath:String) {
    var newRecordsFit:Array[Array[Double]] = _
    var newRecordRows:Array[Row] = _
    var historySolveOld:Array[Row] = _
    var newHistoryRows:Array[Array[Double]] = _
    var front:Array[Row] =  _
    var optimizeRatio = 0.0
    var sep = ","

    def init(sep1:String):Unit = {
        sep = sep1
        //@warn historyModelPath存储的信息包括：决策变量、目标变量
        historySolveOld = Utils.load(historyModelPath,sep)
        front = Utils.load(frontPath,sep)
    }

    def getRecordFitness():Unit = {
        newRecordsFit = Fitness.calcFitness(newRecordsIn, conf.exprList, conf.inputColumns)
        newRecordRows = Utils.convertArrayToRows(Utils.hStack(newRecordsIn, newRecordsFit))
    }

    def updateHistoryBestSolve():Unit = {
        val allHistoryRows = Utils.stackRows(newRecordRows, historySolveOld, 0)
        val rowsSize = front(0).length
        val goalsSize = conf.goalSize
        val dvSize = conf.inputColumns.length

        val allFitIn = Utils.sliceRows(allHistoryRows, 0, dvSize)
        val allFitnessIn = Utils.sliceRows(allHistoryRows, dvSize, rowsSize)

        val paretoResults = new Pareto(allHistoryRows, dvSize)
                .pareto()
        //实际为Array[Array[Double]]
        newHistoryRows = Utils.convertRowsToArray(paretoResults)
        Utils.save(paretoResults, historyModelPath, sep)
    }

    def optimizeSuggestion():Array[Row] = {
        val dvSize = conf.inputColumns.length
        val rowsSize = front(0).length
//        newRecordRows只有一行
        val newRecordArray = Utils.convertRowsToArray(newRecordRows)
        val getHistoryBestRow = getBestSolve(newRecordArray,newHistoryRows, dvSize )
        val getFrontRow = getBestSolve(newRecordArray,newHistoryRows, dvSize)

        //需转成Array[Row]
        val historyFitness = Row(getHistoryBestRow.slice(dvSize,rowsSize).toSeq:_*)
        val frontFitness = Row(getFrontRow.slice(dvSize, rowsSize).toSeq:_*)

        if (Pareto.compare(historyFitness, frontFitness) == false ) {
            optimizeRatio = conf.optimizeRatio
        } else {
            optimizeRatio = 0.0
        }

        val suggestedParticleIn = Array( (0 to dvSize - 1)
                .map(x => getHistoryBestRow(x) + optimizeRatio * (getFrontRow(x) - getHistoryBestRow(x)) ).toArray
        )
        val suggestedParticleFit = Fitness.calcFitness(suggestedParticleIn,conf.exprList, conf.inputColumns)

        val goalSize = conf.goalSize
        //计算优化程度
        val optimizedLevel = (0 to goalSize - 1).map(x => -( (suggestedParticleFit(0)(x) - newRecordArray(0)(x) ) / newRecordArray(0)(x) ) ).toArray
        val goalsLevel = (0 to goalSize - 1).map(x => s"goal${x+1}_level")
        val title = conf.inputColumns.union(goalsLevel)
        val value = suggestedParticleIn(0).union(optimizedLevel)
        val suggestion = Utils.convertArrayToRows(Array(title).union(Array(value)) )
        Utils.save(suggestion, "data/suggestion.txt", ",")

        suggestion
    }

    def predict(sep:String):Array[Row] = {
        init(sep)
        getRecordFitness()
        updateHistoryBestSolve()
        val suggestion = optimizeSuggestion()
        suggestion
    }


    //currRow和rows都包括决策变量、目标变量
    //此处currRow是一行，而不是ArrayRow
    def getBestSolve(currRow:Array[Double], rows:Array[Array[Double]], dvSize:Int):Array[Double] = {
        val rowsSize = rows.length
        val sortedArray =  (0 to rowsSize - 1)
                .map(x => (x, calcAngle(currRow.slice(dvSize,rowsSize),rows(i).slice(dvSize,rowsSize)) ))
                .toArray
                //按第二列：最小角度，升序
                .sortBy(x => x._2)
        val minAngleIndex = sortedArray(0)._1
        rows(minAngleIndex)
    }

    //计算两行的角度（目标空间向量的角度）
    def calcAngle(first:Array[Double], second:Array[Double]):Double = {
        def dotV(vec1:Array[Double],vec2:Array[Double]):Double = {
            (0 to vec1.length - 1).map(x => vec1(x)*vec2(x) ).sum
        }
        val L2First = math.sqrt(dotV(first,first))
        val L2Second = math.sqrt(dotV(second,second))
        val dotFirstAndSecond = math.sqrt(dotV(first,second))
        val cosine = dotFirstAndSecond / (L2First * L2Second)
        math.acos(cosine)
    }

}





object Fitness {
    def calcFitness(in:Array[Array[Double]], exprList:Array[String], fieldNames:Array[String]):Array[Array[Double]] = {???}

    //  仅用于第一次计算适应度
    def evaluateFitnessFirst(input:Array[Array[Double]],conf:Config):Array[Array[Double]] = {
        fitness = Fitness.calcFitness(input,conf.exprList,conf.inputColumns )

    }
}


//加val才表示是该类的成员变量，不能仅仅只在构造器中传递参数而已
class MeshCrowd (var currArchivingIn:Array[Array[Double]],
                 var currArchivingFit:Array[Array[Double]],
                 var meshDiv:Long,
                 var min:Array[Double],
                 var max:Array[Double],
                 var particles:Long) {
    var num = currArchivingIn.length
    var idArchiving = new Array[Long](num)
  //  此处应为2维，使用ofDim
    var crowdArchiving = new Array[Long](num)
  //  此处应为2维
    var probabilityArchiving = new Array[Double](num)
    var gbestIn = Array.ofDim(particles, currArchivingIn(0).length)
    var gbestFit = Array.ofDim(particles,currArchivingFit(0).length)



    def calcMeshId(in:Array[Double]):Long = {
        var id = 0
        val size = currArchivingFit(0).length
//      计算网格id
        id = (0 to size - 1 ).toArray
            .map(x => ( (in(i) - min(i)) * num / (max(i) - min(i)) ).toInt * (math.pow(meshDiv, x)) )
            .reduce(_+_)
        id
    }

    def divideArchiving():Unit = {
        (0 to num - 1).toArray
//                map函数可以传入赋值
                .map(x => idArchiving(x) = calcMeshId(currArchivingFit(x)))
    }

    def getCrowd():Unit = {
        val index = (0 to num - 1).toArray

//        形如Map(2 -> 1, 5 -> 3, 4 -> 2, 1 -> 1)
        val map = idArchiving.map(x=> (x,1))
                    .groupBy(_._1)
                    .map(x =>  (x._1, x._2.size))
//        若idArchiving形如Array(5,5,3,5,2,1),则crowArchiving形如Array(3,3,1,3,1,1),即对应顺序下的词频统计
        crowdArchiving = (0 to idArchiving.length - 1)
                    .map(x => map.get(idArchiving(x)) match {
                        case Some(a) => a
                    })
    }
}


class GetGbest extends MeshCrowd {

    def getProbability():Unit = {
        val probabilityArray = (0 to num - 1)
                    .map(x => 10.0 / math.pow(crowdArchiving(x), 3 ) )
        val sum = probabilityArray.reduce(_+_)
//        math.pow计算出为Double型
        probabilityArchiving = probabilityArray.map(x => x / sum)
    }

    def getGbestIndex(num:Int):Int = {
        var index = 0
        val rand = math.random()
        for(i<- 0 to num - 1) {
            breakable(
                if (rand <= probabilityArchiving.slice(0, i + 1)) {
                    index = i
                    break()
                }
            )
        }
        index
    }

    def getGbest():Tuple2[Array[Array[Double]],Array[Array[Double]]] = {
        getProbability()
        for (i<-0 to particles - 1) {
            val gbestIndex = getGbestIndex()
            gbestIn(i) = currArchivingIn(gbestIndex)
            gbestFit(i) = currArchivingFit(gbestIndex)
        }
        (gbestIn,gbestFit)
    }
}

class ClearArchiving extends MeshCrowd {
    var thresh:Long = _
//    也需要归一化
    def getProbability():Unit = {
        val powerTwo = crowdArchiving.map(x => math.pow(x,2))
        val sum = powerTwo.reduce(_+_)
        probabilityArchiving = (0 to num - 1).map(x => powerTwo(x) / sum )
    }

    def getClearIndex():Array[Int] = {
        val clearSize = currArchivingIn.length - this.thresh
        val clearIndex = new ArrayBuffer[Int]()
        while (clearIndex.length < clearSize) {
            val rand = math.random
            for(i<- 0 to num - 1 if (rand <= probabilityArchiving.slice(0, i + 1).reduce(_+_) ) && !clearIndex.contains(i)) {
                clearIndex.append(i)
            }
        }
        return clearIndex.toArray
    }
//    超过外部储备集容量时，删除表现"不好"的粒子
    def clear(thresh:Long):Tuple2[Array[Array[Double]], Array[Array[Double]]] = {
        this.thresh = thresh
        this.getProbability()
        val clearIndex = getClearIndex()
        val currArchivingInClone = currArchivingIn.clone()
        val currArchivingFitClone = currArchivingFit.clone()
        currArchivingIn = (0 to currArchivingIn.length - 1).filter(x => !clearIndex.contains(x)).toArray.map(x=> currArchivingInClone(x))
        currArchivingFit = (0 to currArchivingFit.length - 1).filter(x => !clearIndex.contains(x)).toArray.map(x=> currArchivingFitClone(x))
        (currArchivingIn, currArchivingFit)
    }
}

object init {

    def initV(particles:Int, vMax:Array[Double], vMin:Array[Double]): Array[Array[Double]] = {
        val size = vMax.length
        val v = Array.ofDim[Double](particles, size)
                .map(row => row.indices.toArray.map(colIndex => math.random * (vMax(colIndex) - vMin(colIndex)) + vMin(colIndex) ))
//        Utils.convertArrayToRows(v)
        v
    }

    def initPbest(in:Array[Row], fitness:Array[Row]):Tuple2[Array[Array[Double]],Array[Array[Double]]] = {
        Tuple2(Utils.convertRowsToArray(in), Utils.convertRowsToArray(fitness))
    }

    //inputRows 包含决策变量和Fitness变量横向连接，形成新的Array[Row]
    //返回结果形同inputRows，是in和fitness的横向连接
    def initArchiving(inputRows:Array[Row], fitColFrom:Int):Tuple2[Array[Array[Double]],Array[Array[Double]]] = {
        val pareto = new Pareto(inputRows, fitColFrom)
        val result = pareto.pareto()
        val archivingIn = Utils.convertRowsToArray(result).map(x => x.slice(0, fitColFrom))
        val archivingFit = Utils.convertRowsToArray(result).map(x => x.slice(fitColFrom, result(0).length))
        (archivingIn, archivingFit)
    }

    def initGbest(currArchivingIn:Array[Array[Double]],
                 currArchivingFit:Array[Array[Double]],
                  meshDiv:Long,
                  min:Array[Double],
                  max:Array[Double],
                  particles:Long):Tuple2[Array[Array[Double]],Array[Array[Double]]] = {
        val getGbest = new GetGbest(currArchivingIn, currArchivingFit, meshDiv, min, max, particles)
        getGbest.getGbest()
    }
}

//由于init和update之间的函数模块，有序执行，故不使用Array[Row]，而是用Array[Array[Double]]或者Array[Double]保存数据
object update {
//    vMin、vMax、inPbest、inGbest的Array长度为1，即1行, 将转为Array[Double]
//    v、in、长度>=1,即为n，将转换为Array[Array[Double]]
    def updateV(v:Array[Array[Double]], vMin:Array[Double], vMax:Array[Double], in:Array[Array[Double]], inPbest:Array[Array[Double]], inGbest: Array[Array[Double]], w:Double, c1:Double, c2:Double):Array[Array[Double]] = {
//    def updateV(v:Array[Row], vMin:Array[Row], vMax:Array[Row], in:Array[Row], inPbest:Array[Row], inGbest: Array[Row], w:Double, c1:Double, c2:Double):Array[Array[Double]] = {
        //集中转成Array[Array[Double]]

        //当输入为Array[Row]时，速度更新如下
//        val params1 = Array(v,in)
//                .map(x => Utils.convertRowsToArray(x))
//        val params2 = Array(vMin, vMax, inPbest, inGbest)
//                .map(x => Utils.rowToArrayDouble(x))
//        val rand1 = math.random
//        val rand2 = math.random
//
//    //Array[Tuple2]中对Tuple2操作的方法：val b = a.map( x =>  x match { case (x,y) => x +y })， 需要使用匿名函数
//        val vNextGen = Utils.genNdArrayIndex(v.length, v(0).length)
////            .map(
////                x => w * params1(0)(x._1)(x._2) + c1 * r1 * ( params2(2)(x._2) - params1(1)(x._1)(x._2) ) + c2 * r2 * (params2(3)(x._2) - in(x._1)(x._2) )
////            )
//                 .map(x => x.map( tuple => tuple match {
//                                case (x,y) =>w * params1(0)(x)(y) + c1 * r1 * ( params2(2)(y) - params1(1)(x)(y) ) + c2 * r2 * (params2(3)(y) - in(x)(y) )
//                                })
//                )

        //Array[Tuple2]中对Tuple2操作的方法：val b = a.map( x =>  x match { case (x,y) => x +y })， 需要使用匿名函数
        val vNextGen = Utils.genNdArrayIndex(v.length, v(0).length)
                //            .map(
                //                x => w * params1(0)(x._1)(x._2) + c1 * r1 * ( params2(2)(x._2) - params1(1)(x._1)(x._2) ) + c2 * r2 * (params2(3)(x._2) - in(x._1)(x._2) )
                //            )
                .map(x => x.map( tuple => tuple match {
                    case (x,y) =>w * v(x)(y) + c1 * r1 * ( inPbest(x)(y) - in(x)(y) ) + c2 * r2 * (inGbest(x)(y) - in(x)(y) )
                    })
                )


        for(i<- 0 to v.length - 1) {
            for(j<- 0 to v(0).length - 1) {
                if(vNextGen(i)(j) < vMin(j) ) {
                    vNextGen(i)(j) = vMin(j)
                }else if (vNextGen(i)(j) > vMax(j)) {
                    vNextGen(i)(j) = vMax(j)
                }

            }
        }
        vNextGen
    }

    def updateIn(in:Array[Array[Double]], v:Array[Array[Double]], inMin:Array[Double], inMax:Array[Double]):Array[Array[Double]] = {
        val rowSize = in.length
        val colSize = in(0).length
        val inNextGen = Utils.genNdArrayIndex(rowSize, colSize)
                .map(array => array.map( tuple => tuple match {
                    case (x,y) => in(x)(y) + v(x)(y)
                }) )

        for (i<-0 to rowSize - 1 ) {
            for (j<- 0 to colSize - 1) {
                if (inNextGen(i)(j) < inMin(j)) {
                    inNextGen(i)(j) = inMin(j)
                } else if (inNextGen(i)(j) > inMax(j)) {
                    inNextGen(i)(j) = inMax(j)
                }
            }
        }
        inNextGen
    }


    def comparePbest(inIndividual:Array[Double],pbestIndividual:Array[Double]):Boolean = {
        var greater = 0
        var less = 0
        (0 to inIndividual.length - 1).toArray
                .map(x => x match {
                    case x if inIndividual(x) > pbestIndividual(x) => greater = greater + 1
                    case x if inIndividual(x) < pbestIndividual(x) => less = less + 1
                })
//        println(s"greater is $greater")
//        println(s"less is $less")
        val rand = math.random
        val flag = (greater, less) match {
            case (x,y) if (x > 0 && y == 0) => false
            case (x,y) if (x == 0 && y > 0) =>true
            case _ =>  rand match {
                case z if z > 0.5 => true
                case z if z < 0.5 => false
            }
        }
        flag
    }

    def updatePbest(in:Array[Array[Double]],
                    fitness:Array[Array[Double]],
                    inPbest:Array[Array[Double]],
                    fitnessPbest:Array[Array[Double]]):Tuple2[Array[Array[Double]],Array[Array[Double]]] = {
        val arrayTuple = (0 to fitnessPbest.length - 1).toArray
                .map(x => x match {
                    case x if comparePbest(fitness(x),fitnessPbest(x)) => (in(x), fitness(x))
                    case x if !comparePbest(fitness(x),fitnessPbest(x)) => (inPbest(x),fitnessPbest(x))
                })
        (arrayTuple.map(x=>x._1), arrayTuple.map(x=>x._2))
    }

    def updateArchiving(in:Array[Array[Double]],
                        fitness:Array[Array[Double]],
                        archivingIn:Array[Array[Double]],
                        archivingFit:Array[Array[Double]],
                        thresh:Int,
                        meshDiv:Int,
                        min:Array[Double],
                        max:Array[Double],
                        particles:Int):Tuple2[Array[Array[Double]],Array[Array[Double]]] = {
        val dvSize = in.length
        val inputs = Utils.convertArrayToRows(Utils.stack(in, fitness))
        val pareto1 = new Pareto(inputs, dvSize)
        val firstPareto = pareto.pareto()
        val oldArchivingRows = Utils.convertArrayToRows(Utils.hStack(in, fitness))
        val concatArchingRows = Utils.stackRows[Double](oldArchivingRows, firstPareto, 0)

        val pareto2 = new Pareto(concatArchingRows, dvSize)
        val newArchiving = Utils.convertRowsToArray(pareto2.pareto())

        var currArchivingIn = newArchiving.map(x => x.slice(0, dvSize))
        var currArchivingFit = newArchiving.map(x => x.slice(dvSize,x.length))
        //超过容量，则删除性能“更差”的粒子
        if (newArchiving.length > thresh) {
            val clear = new ClearArchiving(currArchivingIn, currArchivingFit, meshDiv, min, max, particles)
            val clearedArchiving = clear.clear(thresh)
            currArchivingIn = clearedArchiving._1
            currArchivingFit = clearedArchiving._2
        }
        return (currArchivingIn, currArchivingFit)

    }

    def updateGbest(archivingIn:Array[Array[Double]],
                    archivingFit:Array[Array[Double]],
                    meshDiv:Int,
                    min:Array[Double],
                    max:Array[Double],
                    particles:Int):Tuple2[Array[Array[Double]],Array[Array[Double]]] = {
        val getGbest = new GetGbest(archivingIn, archivingFit, meshDiv, min, max, particles)
        getGbest.getGbest()
    }
}

class Config() {
  var dataPath:String = _
  var jsonPath:String = _
  var goalSize = 0
  var goalInfo:ArrayBuffer[(Int,Array[String],String)] = new ArrayBuffer()
  var conf:JSONObject = _
  var data:Array[Row] = _
  var exprList: Array[String] = _
  var optimizeRatio:Double = _
//  输入数据的字段名
  var inputColumns:Array[String] = _
  //初始化后的schema，是已经修改过字段名的schema，是仅仅修改了字段名的“读取csv的df的schema”
  var schema: StructType = _
  @transient
  var spark = SparkSession.builder().getOrCreate()


  def init():this.type = {
    //主要先获取所有输入字段名
    readData()
    var jsonStr = Source.fromFile(jsonPath).mkString
    //先将json文件的“非正规字段名”替换
    for(field<-inputColumns) {
      jsonStr = jsonStr.replaceAll(field,Utils.modifyFieldName(field))
    }
    println("jsonStr is--------------------------------------")
    println(jsonStr)
    //再将所有原输入数据的字段名替换
    inputColumns = inputColumns.map(column => Utils.modifyFieldName(column))
    println("inputColumns are ------------------------------")
    println(inputColumns.mkString(","))
    //将schema也替换，主要是替换其字段名部分，重新定义一个schema即可
    val fields = schema.fields.map(x => StructField(Utils.modifyFieldName(x.name),x.dataType,x.nullable))
    schema = StructType(fields)

    conf = JSONObject.fromObject(jsonStr)
    goalSize = conf.getJSONArray("goals_function").length()
    optimizeRatio = conf.getDouble("optimizeRatio")
    this
  }

  //获取“每个目标的（id,决策变量，目标函数计算表达式）三元组，方便索引
  def getGoalInfo():this.type = {
    val goalsArray = conf.getJSONArray("goals_function")
    for(i<-0 to goalSize - 1) {
//      JSONObject的getSome(key)方法是指返回结果为Some类型，其中key一定是字符串
      val id = goalsArray.getJSONObject(i).getJSONObject("goal").getInt("id")
      val decisionVariables = goalsArray.getJSONObject(i).getJSONObject("goal").getJSONArray("decision_variables").toArray.map(x=>x.asInstanceOf[String])
      val expr = goalsArray.getJSONObject(i).getJSONObject("goal").getString("expr")
      goalInfo.append((id,decisionVariables,expr))
    }
    this
  }
  //此处字段名还没有修改
  def readData():this.type = {
    val df = spark.read.option("header",true).csv(dataPath)
//    获取输入数据的字段名，用于后续的表达式计算和字段选择
    inputColumns = df.columns
    schema = df.schema
    data = df.collect()
    this
  }

  def process():this.type = {
    this.init()
      .getGoalInfo()
      .getExprList()
  }

  def getExprList(): this.type = {
//    val buffer = new ArrayBuffer[String]()
//    for(i<-0 to goalSize) {
//      buffer.append(goalInfo.toArray.apply(i)._3)
//    }
    exprList = goalInfo.toArray.map(x => x._3)
    println(s"exprList is -----------------------------")
    println(exprList.mkString(","))
//    exprList = buffer.toArray
    this
  }

  def getConf():Tuple2[Array[String],Array[String]] = {
      (inputColumns,exprList)
  }


  def setJsonPath(jsonPath: String):this.type = {
    this.jsonPath = jsonPath
    this
  }
  def getJsonPath(): String = this.jsonPath

  def setDataPath(dataPath: String): this.type = {
    this.dataPath = dataPath
    this
  }

  def getDataPath(): String = this.dataPath
}

object Utils {
  //读取逗号分隔文件，获取Array[Row]
  //Row中的元素为Any,是因元素类型可以不同，在Array中类型不同，也是Array[Any]
  def load(path:String, sep:String):Array[Row] = {
      val file = Source.fromFile(path)
      val array = file.getLines().toArray[String]
              .map(x => x.split(sep))
      file.close()
      array.map(x => Row(x:_*) )
  }

  // 此处支持任意类型的写入
  def save(array:Array[Row],path:String, sep:String):Unit = {
      val writer = new PrintWriter(new File(path))
      array.map(x => x.mkString(sep)).foreach(x => writer.println(x))
  }

  def rowToArrayDouble(row:Row):Array[Double] = {
      row.toSeq.toArray
  }

  def arrayDoubleToRow(array: Array[Double]): Row = {
      Row(array.toSeq:_*)
  }
  // Array[Row]转成Array[Array[Double]]
  def convertRowsToArray(rows:Array[Row]):Array[Array[Double]] = {

      rows.map(x => rowToArrayDouble(x))
  }
  // Array[Array[Double]] 转成 Array[Row]
  def convertArrayToRows(array:Array[Array[Double]]):Array[Row] = {
      array.map(x => arrayDoubleToRow(x))
  }

//    生成二维坐标id，二维Array中的元素是Tuple2,（x,y)，分别对应行、列坐标
  def genNdArrayIndex(rowSize:Int, colSize:Int): Array[Array[Tuple2[Int,Int]]] = {
      (0 to rowSize - 1).toArray.map(x => (0 to colSize - 1).toArray.map( y => (x, y) ) )
  }

  //类似于numpy.hstack
  //要求first和sencond的行数相同，否则报错，暂未添加try catch
  def hStack(first:Array[Array[Double]], second: Array[Array[Double]]):Array[Array[Double]] = {
//      (0 to first.length - 1).toArray.map(rowIndex => first(rowIndex).union(second(rowIndex)) )
      stack(first, second, 1)
  }

//  def vStack(first:Array[Array[Double]], second: Array[Array[Double]]):Array[Array[Double]] = {
//      (0 to first)
//  }

   def stack(first:Array[Array[Double]], second:Array[Array[Double]], axis:Int): Array[Array[Double]] = {
       var stacked:Array[Array[Double]] = axis match {
           case axis if axis == 0 => first.union(second)
           case axis if axis == 1 => (0 to first.length - 1).toArray.map(rowIndex => first(rowIndex).union(second(rowIndex)))
       }
//       if (axis == 0) {
//           first.union(second)
//       } else if (axis == 1) {
//           (0 to first.length - 1).toArray.map(rowIndex => first(rowIndex).union(second(rowIndex)))
//       }
       stacked
   }

   //Row输入的数据是什么类型（需要是单一类型的行），输出得到的Array[Row]获取时，就用什么方法-输入Int,则Row.getInt(i)
   //可能有在获取Array[Row]里的数据时出现问题，此时Row中数据为Any类型
   //即类型T原本是什么类型，变换后只能转为该类型
   def stackRows[T](first:Array[Row], second:Array[Row], axis:Int):Array[Row] = {
       val stacked = axis match  {
           case axis if axis == 0 => first.union(second)
           case axis if axis == 1 => (0 to first.length - 1).toArray
                   .map(x => Row.fromSeq( first(x).toSeq.union(second(x).toSeq).map(x => x.asInstanceOf[T]))
                   )
       }
       stacked
   }

   def vStack(first:Array[Array[Double]], second:Array[Array[Double]]): Array[Array[Double]] = {
       stack(first, second, 1)
   }

   //取第0-11列,共取12列，则from=0， until=11
   def sliceRow(row:Row, from:Int, until:Int):Row = {
       Row(row.toSeq.slice(from,until).toSeq:_*)
   }

   def  sliceRows(rows:Array[Row], from:Int,until:Int):Array[Row] = {
       rows.map(row => sliceRow(row, from, until))
   }

  def loadConf(dataPath:String, jsonPath:String):Config = {
    val conf = new Config()
    conf.setDataPath(dataPath)
    conf.setJsonPath(jsonPath)
    conf.process()
    conf
  }

//  求粒子的初始最大边界、最小边界(还是按照位置边界，而非fitness边界，防止粒子出界
  def getBoundaries(conf: Config, spark:SparkSession):(Array[Double], Array[Double]) ={
//    原始数据Array[Row]转RDD
    val rdd = spark.sparkContext.parallelize(conf.data)
    val fieldsLength = conf.schema.length
    val df = spark.createDataFrame(rdd, conf.schema)
//    获取各维最大边界
    val maxBounds = df.select(df.columns.map(x => max(x) ):_*).collect()(0).toSeq.asInstanceOf[Array[Double]]
//    获取各维最小边界
    val minBounds = df.select(df.columns.map(x => min(x) ):_*).collect()(0).toSeq.asInstanceOf[Array[Double]]
    (maxBounds, minBounds)
  }

//  def computeFitness()

  //将字段名中所有包含+、-、*、/、@的所有字符都替换成下划线，方便expr(expr)解析不报错
  //在配置文件阶段完成即可
  def modifyFieldName(name:String): String = {
    name.replaceAll("\\+|-|\\*|/|@","_")
  }

  //分布式计算Pareto——注,此处没有键，相对原始数据，行的位置会发生变化
  def paretoDataFrame(df:DataFrame, spark:SparkSession): DataFrame = {
    val rddPareto = df.rdd.mapPartitions[Row](iter =>{
      val obj = new Pareto(iter.toArray)
      obj.pareto().toIterator
    })
    val schema = df.schema
    spark.createDataFrame(rddPareto, schema )
  }
}

object Main{
    def train():Unit = {
        val w = 0.8
        val c1 = 0.3
        val c2 = 0.3
        val maxIter = 30
        val meshDiv = 10
        val thresh = 300

        val sparkConf =new SparkConf().setAppName("MOPSO").setMaster("local[*]")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        val dataPath = "data/dataset.csv"
        val jsonPath = "conf/fitness_list.json"
        val historyPath = "data/history.txt"
        val frontPath = "data/front.txt"

        val conf = Utils.loadConf(dataPath, jsonPath)
        //bounds = (maxBounds, minBounds)
        val bounds = Utils.getBoundaries(conf, spark)
        val in = Utils.convertRowsToArray(conf.data)
        val fitness = Fitness.evaluateFitnessFirst(in,conf)
        //决策变量个数
        val dvSize = in(0).length

        val inputsOrigin = Utils.hStack(in,fitness)
        val inputs = Utils.convertArrayToRows(inputsOrigin)
        val paretoInit = new Pareto(inputs, dvSize)

        val paretoResult = Utils.convertRowsToArray(paretoInit.pareto())
        val firstIn = paretoResult.map(x => x.slice(0, dvSize))
        val firstFit = paretoResult.map(x => x.slice(dvSize, paretoResult(0)length))
        //@warn 1 如何写入"data/history.txt"
        Utils.save(paretoResult,historyPath,",")

        val particles = paretoResult.length
        //进入MOPSO算法
        val mopso = new Mopso(particles, w, c1, c2, bounds._1, bounds._2, thresh, meshDiv, conf, firstIn, firstFit)
        //mopso计算过程
        val frontResult = mopso.process(maxIter)
        val front = Utils.convertArrayToRows(Utils.hStack(frontResult._1,frontResult._2))

        //@warn 2：front保存到本地
        Utils.save(front,frontPath,",")
        println("done------------------------------------------------------------")
    }

    def predict(conf:Config):Array[Row] = {
        val inputs = Array(
            Array(517.8949,2076.314,58.81879,518.4253,518.5405,0.8460992,170.3783,190.4063,0.8496983,4232.092,2682.538,522.1612,2852.043,162.719,3.500751,75145.9)
        )
        val dataPath = "data/dataset.csv"
        val jsonPath = "conf/fitness_list.json"
        val historyPath = "data/history.txt"
        val frontPath = "data/front.txt"
        val sep = ","
        val conf = Utils.loadConf(dataPath, jsonPath)
        val result = new Predict(inputs,conf,historyPath,frontPath).predict(sep)

        println("predict finished ---------------------------------------")
        result
    }
}

object myUDF {
  //根据运算符号类型计算
  def ops(opsType:String,number1:Double,number2:Double):Double = {
    var x = 0.0
    x = opsType match {
      case "+" => number1 + number2
      case "-" => number1 - number2
      case "*" => number1 * number2
      case "/" => number1 / number2
//      case "**" => number1 ** number2
    }
    x
  }
}