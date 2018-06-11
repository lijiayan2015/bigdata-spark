package com.ljy.sparkstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

object WindowOprationWC {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val conf = new SparkConf().setMaster("local[2]").setAppName("WindowOprationWC")
    val ssc = new StreamingContext(conf, Milliseconds(5000))

    //设置检查点,因为需要用检查点记录记录历史批次结果数据
    ssc.checkpoint("hdfs://h1:9000/spark/sparkstreaming/cp2018-0611-4")

    val dStream: ReceiverInputDStream[String] = ssc.socketTextStream("h1", 6666)
    val tups: DStream[(String, Int)] = dStream.flatMap(_.split(" ")).map((_, 1))

    val res: DStream[(String, Int)] = tups.reduceByKeyAndWindow((x: Int, y: Int) => (x + y), Seconds(10), Seconds(10))
    res.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
