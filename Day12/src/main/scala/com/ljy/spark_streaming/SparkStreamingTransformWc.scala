package com.ljy.spark_streaming

import com.ljy.sparkstream.LoggerLevels
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
  * 基于DStream的Transform算子的单词计数
  */
object SparkStreamingTransformWc {

  def main(args: Array[String]): Unit = {
    //日志过滤
    LoggerLevels.setStreamingLogLevels()

    val conf = new SparkConf()
      .setAppName("SparkStreamingTransformWc")
      //这里至少需要两个线程
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Milliseconds(5000))

    //设置检查点,单次操作,这里不需要设置检查点
    //ssc.checkpoint("hdfs://h1:9000/spark/sparkstreaming/SparkStreamingTransformWc")

    val data: ReceiverInputDStream[String] = ssc.socketTextStream("h1", 6666)

    val res: DStream[(String, Int)] = data.transform(
      //这里就是操作Spark的基础算子
      _.flatMap(_.split(" ").map((_, 1))).reduceByKey(_ + _))
    res.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
