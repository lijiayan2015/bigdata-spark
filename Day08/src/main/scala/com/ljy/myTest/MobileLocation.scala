package com.ljy.myTest

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  *
  * 用户数据:
  * 电话          时间            基站ID                        状态:1表示进入,0表示离开
  * 18688888888,20160327082400,16030401EAFB68F1E3CDF819735E1C66,1
  *
  * 基站数据:
  * 基站ID                            lng(经度)  lat(纬度)
  * 9F36407EAD8829FC166F14DDE7970F68,116.304864,40.050645,6
  *
  * 需求:
  * 在一定的时间范围内,求经过所有的基站的用户停留的总时长的top2
  *
  * 思路:
  * 1.读取本地日志文件
  * 2.拆分数据,求出用户在相同基站停留的总时长
  *
  * 3.读取经纬度数据,将用户数据与经纬度数据做join操作
  * 4.求经过每一个基站的用户停留的总时长的top2
  *
  */
object MobileLocation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MobileLocation").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //读取本地用户数据
    val userData: RDD[String] = sc.textFile("F://work/java/IdeaProjects/BigData-Spark/Day08/note/log")

    //将用户数据拆分
    val phoneAndLacAndTime = userData.map(line => {
      val infos: Array[String] = line.split(",")
      //电话
      val phone = infos(0)
      //进入或者离开的时间
      val time = infos(1).toLong
      //基站的ID
      val lac = infos(2)
      //用户进入或者离开的状态
      val eventType = infos(3).toInt
      //计算用户停留的时间
      val time_long = if (eventType == 1) -time else time
      //返回数据格式
      ((phone, lac), time_long)
    })
    //计算用户停留时间的总时长
    val reduced: RDD[((String, String), Long)] = phoneAndLacAndTime.reduceByKey(_ + _)

    //将得到的总时长数据变化成(lac,(phone,time))的格式,方便后面与基站数据进行join操作
    val lacAndPhoneAndTime: RDD[(String, (String, Long))] = reduced.map(line => {
      val phone = line._1._1
      val lac = line._1._2
      val time = line._2
      (lac, (phone, time))
    })

    // 读取基站数据
    val lacData: RDD[String] = sc.textFile("F://work/java/IdeaProjects/BigData-Spark/Day08/note/lac_info.txt")

    //将基站数据拆分
    val lacAndLngAndLat: RDD[(String, (String, String))] = lacData.map(line => {
      val lacInfos: Array[String] = line.split(",")
      //基站ID
      val lac = lacInfos(0)
      val lng = lacInfos(1)
      val lat = lacInfos(2)
      (lac, (lng, lat))
    })

    //将用户数据与基站数据进行join操作
    val joined: RDD[(String, ((String, Long), (String, String)))] = lacAndPhoneAndTime.join(lacAndLngAndLat)

    //以基站为key进行分组,然后取出每个组里面的以time倒序排序的前2名
    val sorted: RDD[(String, List[((String, Long), (String, String))])] = joined.groupByKey().mapValues(_.toList.sortBy(_._1._2).reverse.take(2))

    //输出结果:
    sorted.foreach(item => {
      println(item)
    })

    sc.stop()


    /** 输出结果
      * (16030401EAFB68F1E3CDF819735E1C66,List(((18101056888,97500),(116.296302,40.032296)), ((18688888888,87600),(116.296302,40.032296))))
      * (9F36407EAD8829FC166F14DDE7970F68,List(((18101056888,54000),(116.304864,40.050645)), ((18688888888,51200),(116.304864,40.050645))))
      * (CC0710CC94ECC657A8561DE549D940E0,List(((18101056888,1900),(116.303955,40.041935)), ((18688888888,1300),(116.303955,40.041935))))
      */
  }
}
