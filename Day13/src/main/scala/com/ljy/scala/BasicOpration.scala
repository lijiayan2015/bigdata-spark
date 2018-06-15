package com.ljy.scala

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * 基础操作
  */
object BasicOpration {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val spark = SparkSession
      .builder()
      .appName("BasicOpration")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val emp = spark.read.json("F:\\work\\java\\IdeaProjects\\BigData-Spark\\Day13\\note\\emp\\employee.json")

    emp.persist(StorageLevel.MEMORY_AND_DISK) //cache

    emp.createOrReplaceTempView("employee")

    // 获取spark sql的执行计划
    // 比如执行了一个sql语句来获取dataframe
    // 实际上内部包含了一个逻辑执行计划
    // 在设计执行的时候,首先会通过底层的物理执行计划
    // 比如会做一些优化,还会通过动态代码生成技术来提升执行性能
    //spark.sql("select * from employee where age > 20").explain(true)
    //emp.printSchema()

    //写数据
    //val df: DataFrame = spark.sql("select * from employee where age > 30")
    //df.write.mode("append").json("hdfs://h1:9000/spark2x/BasicOpration")

    //dataframe 和 dataset的转换

    val ds: Dataset[Emp] = emp.as[Emp]
    ds.show()

    ds.toDF().show()

    spark.stop()
  }
}

case class Emp(name:String,age:Long,depId:Long,gender:String,salary:Double)
