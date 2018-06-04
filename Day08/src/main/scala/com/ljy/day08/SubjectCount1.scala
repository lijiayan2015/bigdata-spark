package com.ljy.day08

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计每个学科的各个模块的访问量,再取top(3)
  */
object SubjectCount1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SubjectCount1").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val file: RDD[String] = sc.textFile("F://work/java/IdeaProjects/BigData-Spark/Day08/note/access.txt")

    //切分数据,并返回元组(url,1),这样方便以后进行聚合
    val tulpes: RDD[(String, Int)] = file.map(line => {
      val fields = line.split("\\s")
      val url: String = fields(1)
      (url, 1)
    })

    //将相同的url进行聚合,得到的结果就是相同的学科里的相同模块的访问
    val reduced = tulpes.reduceByKey(_ + _)

    //获取学科信息,返回学科url,访问量:(subject,count)
    val suc = reduced.map(x => {
      val url = x._1
      val count = x._2
      val subject = new URL(url).getHost

      (subject, url, count)
    })

    //俺咋uo学科信息分组
    val gouped: RDD[(String, Iterable[(String, String, Int)])] = suc.groupBy(x => x._1)

    //在学科信息组里面进行降序排序
    val sorted: RDD[(String, List[(String, String, Int)])] = gouped.mapValues(_.toList.sortBy(_._3).reverse)

    val res: RDD[(String, List[(String, String, Int)])] = sorted.mapValues(_.take(3))
    res.foreach(x => {
      println(x._2)
    })
    sc.stop()
  }
}
