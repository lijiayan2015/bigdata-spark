package com.ljy.myTest

object Test {
  def main(args: Array[String]): Unit = {
    val list = List(("tom",2),("ketty",1),("joke",2),("tom",3),("ketty",1))
    val grouped = list.groupBy(_._1)


    val stringToTuple: Map[String, (String, Int)] = grouped.mapValues(_.reduce((x, y)=>(x._1,x._2+y._2)))
    println(stringToTuple)
  }
}
