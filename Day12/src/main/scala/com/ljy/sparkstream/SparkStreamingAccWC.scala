package com.ljy.sparkstream

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
  * 实现按批次累加功能
  */
object SparkStreamingAccWC {

  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingAccWC")
    val ssc = new StreamingContext(conf, Milliseconds(5000))

    //设置检查点,因为需要用检查点记录记录历史批次结果数据
    ssc.checkpoint("hdfs://h1:9000/spark/sparkstreaming/cp2018-0611")

    //获取数据
    val dStream: ReceiverInputDStream[String] = ssc.socketTextStream("h1", 6666)

    //分析数据
    val tupls: DStream[(String, Int)] = dStream.flatMap(_.split(" ")).map((_, 1))

    val res = tupls.updateStateByKey(func,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)
    res.print()
    ssc.start()
    ssc.awaitTermination()
  }

  //k-->
  //updateFunc: (Iterator[(K,   对应每个单词
  // Seq[V],    V对应每个单词对应的值
  // Option[S])])
  // => Iterator[(K, S)],
  //
  //                      元组中的key(一个个的单词)
  //                         |     当前批次单词出现的次数
  //                         |         |       代表一批次累加的结果,因为有可能有值,也有可能没有值,所以用Option来封装
  val func = (it:Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map(x=>{
      (x._1,x._2.sum+x._3.getOrElse(0))
    })
  }
}
