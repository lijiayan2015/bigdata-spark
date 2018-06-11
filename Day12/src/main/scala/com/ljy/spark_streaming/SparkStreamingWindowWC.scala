package com.ljy.spark_streaming

import com.ljy.sparkstream.LoggerLevels
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

/**
  * DStream 的 window 操作
  */
object SparkStreamingWindowWC {

  def main(args: Array[String]): Unit = {

    //日志过滤
    LoggerLevels.setStreamingLogLevels()

    val conf = new SparkConf()
      .setAppName("SparkStreamingWindowWC")
      //这里至少需要两个线程
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Milliseconds(5000))

    //设置检查点:因为需要用检查点记录历史批次的结果数据
    ssc.checkpoint("hdfs://h1:9000/spark/sparkstreaming/SparkStreamingWindowWC")

    val data: ReceiverInputDStream[String] = ssc.socketTextStream("h1", 6666)

    val res: DStream[(String, Int)] = data.flatMap(_.split(" ")).map((_, 1)).reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(10), Seconds(10))

    res.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
