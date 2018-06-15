package com.ljy.sp

import com.ljy.scala.LoggerLevels
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 基础操作
  */
object BasicOpration6 {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("BasicOpration6")
      .getOrCreate()


    import spark.implicits._
    val employee: DataFrame = spark.read.json("F:\\work\\java\\IdeaProjects\\BigData-Spark\\Day13\\note\\emp\\employee.json")
    //缓存
    employee.persist()

    employee.createOrReplaceTempView("emp")

    /**
      * 获取sparksql的执行计划
      * 比如执行了一个sql语句开获取一个DataFrame
      * 实际上内部包含一个逻辑执行计划
      * 在设计执行的时候,首先会通过底层的物理执行计划
      * 比如会做一些优化
      * 还会通过动态代码生成过技术,来提升执行性能
      */
    spark.sql("select * from emp where age > 20").explain()
    employee.printSchema()

    println("=============================")

    // 写数据
    val empDF: DataFrame = spark.sql("select * from emp where age > 30")
    //empDF.write.json("hdfs://h1:9000/spark2x/BasicOpration6")


    //dataset 和 dataframe相互转换
    val empDS: Dataset[Emp] = empDF.as[Emp]
    empDS.show()
    println("========================")
    val empDF2: DataFrame = empDS.toDF()
    empDF2.show()
    spark.stop()
  }
}

case class Emp(name:String,age:Long,depId:Long,gender:String,salary:Double)