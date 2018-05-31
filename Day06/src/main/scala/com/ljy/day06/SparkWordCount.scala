package com.ljy.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object SparkWordCount {
  def main(args: Array[String]): Unit = {
    //创建配置信息
    //setAppName :设置应用程序名称
    //setMaster:设置为班底运行,需要指定线程数
    //其中:"local"是指单线程来运行
    // "local[2]"是指调用两个线程模拟集群
    //"local[*]"是指有多少空闲线程就用多少
    val conf = new SparkConf()
      .setAppName("SparkWordCount")
      //.setMaster("local[2]")

    //创建上下文对象
    val sc: SparkContext = new SparkContext(conf)
    //读取数据
    val lines: RDD[String] = sc.textFile("hdfs://h1:9000/spark/wc")

    //将数据切分并压平
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //将单词生成元组
    val tuples: RDD[(String, Int)] = words.map((_, 1))
    //将数据聚合
    val reduced: RDD[(String, Int)] = tuples.reduceByKey(_ + _)

    //降序排序,true:表示升序,false 降序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    //collect将RDD转化成数组
    println(sorted.collect().toBuffer)
    //将数据存到hdfs
    sorted.saveAsTextFile("hdfs://h1:9000/spark/wc/out/out_20180531")
    sc.stop()
  }
}
