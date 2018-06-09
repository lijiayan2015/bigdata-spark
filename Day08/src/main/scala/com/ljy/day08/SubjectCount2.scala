package com.ljy.day08

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SubjectCount2 {
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


    val subjects = Array("http://android.learn.com",
      "http://ui.learn.com",
      "http://h5.learn.com",
      "http://java.learn.com",
      "http://bigdata.learn.com")


    //将相同的url进行聚合,得到的结果就是相同的学科里的相同模块的访问量
    val reduced = tulpes.reduceByKey(_ + _)

    //在发生shuffle后的得到的中间数据往往很重要,会经常调用该数据
    //为了提高调用速度,可以把该数据缓存起来
    //注意cache的数据类特别大的时候,会造成整个任务出现oom
    val cached: RDD[(String, Int)] = reduced.cache() //或者调用persist

    for (subject <- subjects) {
      println("============================"+subject)
      val filtedSubject: RDD[(String, Int)] = cached.filter(_._1.startsWith(subject))
      val sorted = filtedSubject.sortBy(_._2, false)
      val res = sorted.take(3)
      println(res.toBuffer)
    }


    sc.stop()
  }
}
