package com.ljy.day09

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 实现自定义分区器,按照学科把不同的学科结果信息放到响应的分区里
  */
object SubjectCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SubjectCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //获取数据
    val file = sc.textFile("F://work/java/IdeaProjects/BigData-Spark/Day08/note/access.txt")

    val tupled: RDD[(String, Int)] = file.map(line => {
      val fields = line.split("\\s")
      val url = fields(1)
      (url, 1)
    })

    val sumed: RDD[(String, Int)] = tupled.reduceByKey(_ + _)


    val subjectAndURLAndCount = sumed.map(x => {
      val url = x._1
      val count = x._2
      val sub = new URL(url).getHost
      (sub, (url, count))
    })


    /**
      * 调用默认分区器进行分区,会发生数据倾斜的问题
      */

    //缓存
    val cached: RDD[(String, (String, Int))] = subjectAndURLAndCount.cache()

    val subjects: Array[String] = cached.keys.distinct().collect()

    //调用系统默认的分区器进行分区
    //val partitioned: RDD[(String, (String, Int))] = cached.partitionBy(new HashPartitioner(5))
    val partitioned = cached.partitionBy(new SubjectPaartitioner(subjects))

    partitioned.mapPartitions(it=>{
      it.toList.sortBy(_._2._2).take(3).iterator
    })

    partitioned.saveAsTextFile("F://work/java/IdeaProjects/BigData-Spark/Day09/note/res")


    sc.stop()
  }


}


class SubjectPaartitioner(subjects: Array[String]) extends Partitioner {
  //用于存储学科以及学科对应的分区号
  val subjectAndNum = new mutable.HashMap[String, Int]()

  override def numPartitions: Int = subjects.length


  var i = 0
  for (subject <- subjects) {
    subjectAndNum += (subject -> i)
    i += 1
  }

  override def getPartition(key: Any): Int = subjectAndNum.getOrElse(key.toString,0)
}