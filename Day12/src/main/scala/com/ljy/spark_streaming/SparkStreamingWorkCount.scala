package com.ljy.spark_streaming

import com.ljy.sparkstream.LoggerLevels
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SparkStream实现单词计数
  *
  * 这里需要用到Natcat服务
  * 1.安装Natcat
  * yum -y install nc
  *
  * 2.启动NC
  * nc -lk 6666
  *
  * 3.在启动进程中输入数据,SparkStreaming就可以从其中获取数据了
  */
object SparkStreamingWorkCount {
  def main(args: Array[String]): Unit = {

    //日志过滤
    LoggerLevels.setStreamingLogLevels()

    val conf = new SparkConf()
      .setAppName("SparkStreamingWorkCount")
      //这里至少需要两个线程
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    //5s中读取读取一次数据,进项一次计算
    val ssc = new StreamingContext(sc, Seconds(5))

    //从简单Socket中读取数据
    val dStream: ReceiverInputDStream[String] = ssc.socketTextStream("h1", 6666)

    //调用DStream的算子
    val res: DStream[(String, Int)] = dStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    //打印计算结果
    res.print()

    //提交任务
    ssc.start()

    // 线程等待,等待下一批次处理
    ssc.awaitTermination()
  }
}
