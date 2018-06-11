package com.ljy.spark_streaming

import com.ljy.sparkstream.LoggerLevels
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}

/**
  * 使用DStream的算子updateStateByKey算子进行累加统计算次
  */
object SparkStreamingAccWc {
  def main(args: Array[String]): Unit = {

    //日志过滤
    LoggerLevels.setStreamingLogLevels()

    val conf = new SparkConf()
      .setAppName("SparkStreamingAccWc")
      //这里至少需要两个线程
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Milliseconds(5000))

    //设置检查点:因为需要用检查点记录历史批次的结果数据
    ssc.checkpoint("hdfs://h1:9000/spark/sparkstreaming/updateStateByKey")

    //获取数据
    val dStream: ReceiverInputDStream[String] = ssc.socketTextStream("h1", 6666)

    //分析数据
    val tupls: DStream[(String, Int)] = dStream.flatMap(_.split(" ")).map((_, 1))

    val res = tupls.updateStateByKey(func, new HashPartitioner(ssc.sparkContext.defaultParallelism), rememberPartitioner = true)

    res.print()
    ssc.start()
    ssc.awaitTermination()
  }


  /**
    * updateStateByKey需要传三个参数：
    * 第一个参数：需要一个具体操作数据的函数，该函数的参数列表传进来一个迭代器
    *   Iterator中有三个类型，分别代表：
    *       String：代表元组中的key，也就是一个个单词
    *       Seq[Int]：代表当前批次单词出现的次数，相当于：Seq(1,1,1)
    *       Option[Int]：代表上一批次累加的结果，因为有可能有值，也有可能没有值，所以用Option来封装,
    *         在获取Option里的值的时候，最好用getOrElse，这样可以给一个初始值
    * 第二个参数：指定分区器
    * 第三个参数：是否记录上一批次的分区信息
    */
  val func = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map {
      /**
        * x表示key
        * y表示当前批次单词出现的次数
        *
        * z代表上一批次累加的结果，因为有可能有值，也有可能没有值，所以用Option来封装,
        * 在获取Option里的值的时候，最好用getOrElse，这样可以给一个初始值
        */
      case (x, y, z) =>
        (x, y.sum + z.getOrElse(0))
    }
  }
}
