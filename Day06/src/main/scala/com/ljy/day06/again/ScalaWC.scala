package com.ljy.day06.again

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScalaWC {
  def main(args: Array[String]): Unit = {

    val fsconf = new Configuration()

    val fs = FileSystem.get(new URI("hdfs://h1:9000"), new Configuration())

    val path = new Path("hdfs://h1:9000/spark/wc/out");
    if (fs.exists(path)) {
      fs.delete(path, true)
    }

    //1.创建配置信息
    val conf = new SparkConf()
      .setAppName("SparkWC")
      .setMaster("local[*]")

    //创建上下文对象
    val sc = new SparkContext(conf)

    //读取数据
    val lines: RDD[String] = sc.textFile("hdfs://h1:9000/spark/wc")

    //拆分数据
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //组件元组(key,1)
    val tups: RDD[(String, Int)] = words.map((_, 1))

    //分组
    val reduced: RDD[(String, Int)] = tups.reduceByKey((x, y) => x + y)

    //排序,false 降序,true升序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)

    sorted.saveAsTextFile("hdfs://h1:9000/spark/wc/out/wc-2018-0601")
    sc.stop()
  }
}
