package com.ljy.sparkstream

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object LoadKafkaDataWc {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingAccWC")
    val ssc = new StreamingContext(conf, Milliseconds(5000))

    //设置检查点,因为需要用检查点记录记录历史批次结果数据
    ssc.checkpoint("hdfs://h1:9000/spark/sparkstreaming/cp2018-0611-3")

    //设置请求kafka的几个必要参数
    val Array(zkQuorm,group,topics,numThreads) = args
    //获取每个topic放到Map里面
    val topicMap: Map[String, Int] = topics.split(",").map((_,numThreads.toInt)).toMap

    //调用kafka工具类来获取kafka集群的数据,其中key为数据的offset值,value就是数据
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zkQuorm,group,topicMap,StorageLevel.MEMORY_AND_DISK)

    //把offset值过滤掉
    val lines: DStream[String] = data.map(_._2)

    //分析数据
    val tups: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1))

    val res = tups.updateStateByKey(func,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)

    res.print()
    ssc.start()
    ssc.awaitTermination()
  }


  val func =  (it:Iterator[(String, Seq[Int], Option[Int])])=>{
    it.map(x=>{
      (x._1,x._2.sum+x._3.getOrElse(0))
    })
  }
}
