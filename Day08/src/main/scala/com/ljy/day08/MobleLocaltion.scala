package com.ljy.day08

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 在一定的时间范围内,求用户经过所有的基站停留的总时长的top2
  */
object MobleLocaltion {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MobleLocaltion")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    //获取数据

    val file: RDD[String] = sc.textFile("F://work/java/IdeaProjects/BigData-Spark/Day08/note/log")
    //切分数据
    val phoneAndLacAndTime: RDD[((String, String), Long)] = file.map(line => {
      val fields = line.split(",")

      val phone = fields(0) //用户唯一表示
      val time = fields(1).toLong
      val lac = fields(2)
      val eventType = fields(3).toInt
      val time_long = if (eventType == 1) -time else time
      ((phone, lac), time_long)
    })
    //用户在相同的基站停留的时长总和
    val sorted: RDD[((String, String), Long)] = phoneAndLacAndTime.reduceByKey(_ + _)

    //为了方便以后把基站ID对应的经纬度信息join进来,需要把上面的数据调整-->把lac作为key
    val lpt: RDD[(String, (String, Long))] = sorted.map(line => {
      val phone = line._1._1 //手机号
      val lac = line._1._2
      val time = line._2 //用户在某个基站停留的总时长
      (lac, (phone, time))
    })

    //获取经纬度信息
    val lac_info: RDD[String] = sc.textFile("F://work/java/IdeaProjects/BigData-Spark/Day08/note/lac_info.txt")

    // 切分基站信息
    val lacxy: RDD[(String, (String, String))] = lac_info.map(line => {
      val fields = line.split(",")
      val lac = fields(0)
      //基站ID
      val x = fields(1) //精度
      val y = fields(2) //维度
      (lac, (x, y))
    })

    // 把经纬度join进来
    val joined: RDD[(String, ((String, Long), (String, String)))] = lpt.join(lacxy)

    val ptxy: RDD[(String, Long, (String, String))] = joined.map(line => {

      val phone = line._2._1._1
      val time = line._2._1._2
      val xy = line._2._2
      (phone, time, xy)
    })

    //手机号分组
    val grouped: RDD[(String, Iterable[(String, Long, (String, String))])] = ptxy.groupBy(_._1)

    //按照时长进行组内排序
    val sortedEnd: RDD[(String, List[(String, Long, (String, String))])] = grouped.mapValues(_.toList.sortBy(_._2).reverse).mapValues(_.take(2))

    println(sortedEnd.collect.toBuffer)

    //(18101056888,List((18101056888,97500,(116.296302,40.032296)), (18101056888,54000,(116.304864,40.050645))))
    //(18688888888,List((18688888888,87600,(116.296302,40.032296)), (18688888888,51200,(116.304864,40.050645))))

  }
}
