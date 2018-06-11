package com.ljy.spark_streaming

import com.ljy.sparkstream.LoggerLevels
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
  * 从kafka获取数据
  */
object SparkStreamKafkaDataWc {

  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val topic = "test"
    val threadNum = 1
    val topics = Map((topic -> threadNum))
    val groupid = "group01"

    val zkList = "h1:2181,h2:2181,h3:2181"
    val conf = new SparkConf().setAppName("SparkStreamKafkaDataWc").setMaster("local[2]")

    val ssc = new StreamingContext(conf, Milliseconds(5000))

    ssc.checkpoint("hdfs://h1:9000/spark/spark/sparkstreaming/SparkStreamKafkaDataWc")

    /**
      * (offset,lines)
      */
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkList, groupid, topics, StorageLevel.MEMORY_AND_DISK)

    val lines: DStream[String] = data.map(_._2)

    val res = lines.flatMap(_.split(" ")).map((_, 1)).updateStateByKey(func, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)

    res.print()
    ssc.start()
    ssc.awaitTermination()
  }


  val func = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map(x => {
      (x._1, x._2.sum + x._3.getOrElse(0))
    })
  }
}
