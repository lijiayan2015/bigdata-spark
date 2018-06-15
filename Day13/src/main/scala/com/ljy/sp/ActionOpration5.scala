package com.ljy.sp

import com.ljy.scala.LoggerLevels
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * action 操作
  */
object ActionOpration5 {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val spark = SparkSession.builder()
      .appName("ActionOpration5")
      .master("local[*]")
      .getOrCreate()

    val emp: DataFrame = spark.read.json("F:\\work\\java\\IdeaProjects\\BigData-Spark\\Day13\\note\\emp\\employee.json")

    import spark.implicits._
    println("===============collect")
    println(emp.collect().toBuffer)
    println("===============count")
    println(emp.count())
    println("===============take")
    println(emp.take(2).toBuffer)
    println("===============first")
    println(emp.first())
    println("===============head")
    println(emp.head(3).toBuffer)
    println("===============foreach")
    emp.foreach(println(_))
    println("===============reduce")
    println(emp.map(x => 1).reduce(_ + _))

    spark.stop()
  }
}
