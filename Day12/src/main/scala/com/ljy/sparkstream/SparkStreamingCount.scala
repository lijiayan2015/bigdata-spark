package com.ljy.sparkstream

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * NetCat
  *
  * 安装nc yum -y install nc
  *
  * nc -lk 6666
  *
  * client------>server
  *
  */
object SparkStreamingCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkStreamingCount").setMaster("local[2]")//这里至少2个线程
    val sc = new SparkContext(conf)

    //创建SparkStreaming的上下文对象
    val ssc: StreamingContext = new StreamingContext(sc,Seconds(5))

    //获取数据
    //获取natCat服务的数据
    val sStreram: ReceiverInputDStream[String] = ssc.socketTextStream("h1",6666)

    //分许数据
    val res: DStream[(String, Int)] = sStreram.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    res.print()

    //提交任务
    ssc.start()
    // 线程等待,等待处理处理下一批次的任务
    ssc.awaitTermination()
  }
}
