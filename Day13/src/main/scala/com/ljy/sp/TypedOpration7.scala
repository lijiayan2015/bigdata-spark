package com.ljy.sp

import com.ljy.scala.LoggerLevels
import org.apache.spark.sql.{DataFrame, SparkSession}

object TypedOpration7 {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("BasicOpration6")
      .getOrCreate()

    import spark.implicits._
    val employee: DataFrame = spark.read.json("F:\\work\\java\\IdeaProjects\\BigData-Spark\\Day13\\note\\emp\\employee.json")
    val empDS = employee.as[Emp]

    println("初始分区数:"+empDS.rdd.partitions.length)

    // coalesce:只能减少分区数
    // repartitions 可以增加分区,也可以减少分区
  }
}
