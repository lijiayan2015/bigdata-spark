package com.ljy.myTest

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 数据源:
  * 20161123101523	http://java.learn.com/java/javaee.shtml
  * java.learn.com:学科
  * javaee:学科下面的模块
  *
  * 需求,求每个学科下面模块的访问量,并取出前3
  */
object SubjectCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SubjectCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //读取数据
    val data: RDD[String] = sc.textFile("F://work/java/IdeaProjects/BigData-Spark/Day08/note/access.txt")

    //拆分数据并组装数据
    val tuples: RDD[(String, (String, Int))] = data.map(line => {
      //由于不确定是空格还是制表符,就用正则\s来包含左右的空白字符
      val infos: Array[String] = line.split("\\s")
      val url = infos(1)

      //获取学科
      val host = new URL(url).getHost

      //组装数据
      (host, (url, 1))
    })

    val grouped: RDD[(String, Iterable[(String, Int)])] = tuples.groupByKey()

    val value: RDD[(String, Map[String, List[(String, Int)]])] = grouped.mapValues(_.toList.groupBy(_._1))

    val values: RDD[(String, Map[String, Int])] = value.mapValues(x => x.mapValues(_.aggregate(0)((x, y) => x + y._2, (x, y) => x + y)))
    val wocaonima = values.mapValues(_.toList.sortBy(_._2).reverse.take(3))
    wocaonima.foreach(println)
    sc.stop()

  }
}
