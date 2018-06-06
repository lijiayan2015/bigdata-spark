package com.ljy.day10

import org.apache.spark.{SparkConf, SparkContext}

object CustomSortDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("CustomSortDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val girlInfo = sc.parallelize(List(("mimi", 80, 35), ("bb", 90, 37), ("hh", 86, 32)))

    //第一种排序方式
    /*val sorted: RDD[(String, Int, Int)] = girlInfo.sortBy(_._2)
    println(sorted.collect().reverse.toBuffer)*/

    import MyPref.girlOrdering
    val sorted = girlInfo.sortBy(goddss => Girl(goddss._1, goddss._2, goddss._3), false)

    println(sorted.collect().toBuffer)


    println("====================")
    val sorted2 = girlInfo.sortBy(goddss => MyGirl(goddss._1, goddss._2, goddss._3), false)
    println(sorted2.collect().toBuffer)

    sc.stop()
  }
}


case class Girl(name: String, face: Int, age: Int)


case class MyGirl(name: String, face: Int, age: Int) extends Ordered[MyGirl] {
  override def compare(that: MyGirl): Int = {
    if (this.face == that.face) {
      this.age - that.age
    } else {
      this.face - that.face
    }
  }
}