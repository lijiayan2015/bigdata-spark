package com.ljy.scala

import org.apache.spark.sql.SparkSession

/**
  * 日期函数:current_date
  * current_timestamp  时间戳
  * 数学函数:
  * round 保留几位小数
  * rand 水机函数
  * 字符串函数:concat concat_ws
  */
object OtherFunction {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val spark = SparkSession
      .builder()
      .appName("BasicOpration")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val emp = spark.read.json("F:\\work\\java\\IdeaProjects\\BigData-Spark\\Day13\\note\\emp\\employee.json")

    import org.apache.spark.sql.functions._
    emp.select(emp("name"),current_date(),current_timestamp(),rand(),round(emp("salary"),2),concat(emp("gender"),emp("age")),concat_ws("|",emp("gender"),emp("age"))).show()

  }
}
