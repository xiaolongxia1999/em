package com.bonc.Main

import java.io.{File, FileInputStream}
import java.util.Properties

import com.bonc.interfaceRaw.IModel

//import com.bonc.Interface.IModel
import org.apache.spark.sql.Row

/**
  * Created by sid on 2018/7/23 0023.
  */

//见https://www.cnblogs.com/AaronCui/p/4915055.html
object ModelsFactory {
  def init(propsPath:String):Properties={
    var props = new Properties();
    props.load(new FileInputStream(new File(propsPath)))
    props
  }

  def produce(shortName:String,props:Properties):IModel[Row]={
//    var iModel: IModel[Row] = IModel[_]
    val fullName = props.getProperty(shortName.toLowerCase)
    val iModel:IModel[Row] = Class.forName(fullName).newInstance().asInstanceOf[IModel[Row]]
    iModel
  }

  def main(args: Array[String]): Unit = {
//    val prop = ModelsFactory.init("conf/default.properties")
//    val alias = prop.getProperty("TE")
//    println(alias)
//    val path = "conf/default.properties"
//    val dirs  = new File("")
//    println(dirs.getAbsolutePath+File.separator+path)



  }



}
