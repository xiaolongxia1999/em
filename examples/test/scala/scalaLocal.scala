import util.control.Breaks._

/**
  * Created by Administrator on 2018/9/26 0026.
  */
object scalaLocal {

  def cp(a:Array[Int], b:Int):Boolean={
//    var flag = true
//
    for(i<-0 to a.length - 1 if (a(i) != b)){
      if (a(i) < b) {
        false
      }
    }
    true
  }

  def main(args: Array[String]): Unit = {
//    val a = 2
//    if (a>3) {} else {println(s"$a")}
    var a = Array(1,2,3)
//    println(cp(a, 1))

    for (i<- a) {
      if (i < 2) {
        a = a.drop(1)
      }
    }
    a.foreach(println)
  }



}
