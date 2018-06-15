package com.ljy.scala

import org.apache.spark.sql.SparkSession

object ActionOpration1 {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val spark = SparkSession.builder()
      .appName("ActionOpration1")
      .master("local[2]")
      //设置Spark sql的元数据仓库的目录
      //.config("spark.sql.warehouse.dir", "F://work/java/IdeaProjects/BigData-Spark/Day13/note/spark-warehouse")
      //启动hive支持
      //.enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val employee = spark.read.json("F:\\work\\java\\IdeaProjects\\BigData-Spark\\Day13\\note\\emp\\employee.json")

    println(employee.collect().toBuffer)
    println(employee.count())
    println(employee.take(2))
    println(employee.first())
    println(employee.head(3))
    employee.foreach(println(_))
    println(employee.map((x => 1)).reduce(_ + _))

    spark.stop()
  }
}
