package com.ljy.onyarn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkOnYarnClientWC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkOnYarnClientWC")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("hdfs://h1:9000/spark/in/")
    val res: RDD[(String, Int)] = lines.flatMap(_.split("\\s")).map((_, 1)).reduceByKey(_ + _)
    res.saveAsTextFile("hdfs://h1:9000/spark/out/onyarn/SparkOnYarnClientWC")
    sc.stop()

  }
}
