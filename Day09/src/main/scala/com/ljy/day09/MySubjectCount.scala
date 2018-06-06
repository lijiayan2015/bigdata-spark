package com.ljy.day09

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 实现自定义分区器
  */
object MySubjectCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MySubjectCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //读取学科数据
    val file = sc.textFile("F:\\work\\java\\IdeaProjects\\BigData-Spark\\Day08\\note\\access.txt")
    //拆分数据
    val subjects: RDD[(String, Int)] = file.map(line => {
      val fields = line.split("\\s")
      val url = fields(1)
      (url, 1)
    })

    val sumed: RDD[(String, Int)] = subjects.reduceByKey(_ + _)
    val suc: RDD[(String, (String, Int))] = sumed.map(line => {
      val url = line._1
      val subject = new URL(url).getHost
      (subject, (url, line._2))
    })

    val cached: RDD[(String, (String, Int))] = suc.cache()
    val keys: Array[String] = cached.keys.collect().distinct

    val partitioned = cached.partitionBy(new SubjectPaartitioner(keys))
    val taked = partitioned.mapPartitions(f=>f.toList.sortBy(_._2._2).reverse.iterator)
    taked.saveAsTextFile("F:\\work\\java\\IdeaProjects\\BigData-Spark\\Day09\\note/repartition_res")
    sc.stop()
  }
}

class SubjectPartitioner(keys: Array[String]) extends Partitioner {

  val subjectAndNum = new mutable.HashMap[String, Int]()

  for (i <- 0 to keys.length) {
    subjectAndNum += (keys(i) -> i)
  }

  override def numPartitions: Int = {
    keys.length
  }

  override def getPartition(key: Any): Int = {
    subjectAndNum.getOrElse(key.toString, 0)
  }
}
