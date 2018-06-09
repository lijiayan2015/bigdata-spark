package com.ljy.day10

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UVDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UVDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //获取数据
    val file = sc.textFile("F:\\work\\java\\IdeaProjects\\BigData-Spark\\Day08\\note\\access.txt")
    //切分并获取ip
    val ip: RDD[String] = file.map(_.split("\\s")(0))
    val count = ip.distinct.count()
    println(count)
    sc.stop()
  }
}
