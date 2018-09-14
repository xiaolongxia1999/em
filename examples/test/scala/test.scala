import udaf.myAggregator

/**
  * Created by Administrator on 2018/9/7 0007.
  */

//主构造器和辅助构造器，主构造器可以有多个参数，可以加入val和var声明
//见：https://blog.csdn.net/Captain72/article/details/78855373
class test {
  var a: String = ""
  var b: Int = 0
  def this(a1: String) {
    this()
    this.a = a1
  }

  def this(name: String, age: Int) {
    this(name)

    this.b = age

  }

  def add: Double = b.toDouble + 1
}

object test2 {
  val a =   1.0

  def fwad(a: Double): myAggregator ={
    new myAggregator
  }

  def main(args: Array[String]): Unit = {

    val test1 = new test()
    val test3 = new test("df")


    println("dj")
  }
}
