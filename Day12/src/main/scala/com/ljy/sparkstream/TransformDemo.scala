package com.ljy.sparkstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object TransformDemo {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingAccWC")
    val ssc = new StreamingContext(conf, Milliseconds(5000))

    //设置检查点,因为需要用检查点记录记录历史批次结果数据
    ssc.checkpoint("hdfs://h1:9000/spark/sparkstreaming/cp2018-0611-2")

    //获取数据
    val dStream: ReceiverInputDStream[String] = ssc.socketTextStream("h1", 6666)

    val res: DStream[(String, Int)] = dStream.transform(rdd => {
      rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    })
    res.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
