package com.ljy.day10

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PVDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PVDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //获取数据
    val file = sc.textFile("F:\\work\\java\\IdeaProjects\\BigData-Spark\\Day08\\note\\access.txt")

    //把每条信息生成元组
    val tupled: RDD[(String, Int)] = file.map(x => ("pv", 1))

    //聚合
    val reduced: RDD[(String, Int)] = tupled.reduceByKey(_ + _)

    reduced.foreach(println)
    sc.stop()
  }
}
