package com.ljy.scala

import org.apache.spark.sql.{Dataset, SparkSession}

object TypeOpration1 {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val spark = SparkSession
      .builder()
      .appName("TypeOpration1")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val emp = spark.read.json("F:\\work\\java\\IdeaProjects\\BigData-Spark\\Day13\\note\\emp\\employee.json")

    //val employeDS: Dataset[Emp] = emp.as[Emp]

    //println("初始分区数:"+employeDS.rdd.partitions.length)

    // coalesce:只能用于减少分区数量,可以选择不发生shuffle

    //  repartition:可以增加分区,也可以减少分区,一定会发生shuffle

    //val empRepartitioned: Dataset[Emp] = employeDS.repartition(8)
    //println("重新分区后的分区数:"+empRepartitioned.rdd.partitions.length)

    //val empCoalesed: Dataset[Emp] = empRepartitioned.coalesce(13)

    //println("重新分区后的分区数:"+empCoalesed.rdd.partitions.length)

    // distinct dropDuplicates 都是去重
    // distinct:根据每条数据进行完成内容的比对进行去重
    // dropDuplicates:可以根据指定的字段进行去重
    //employeDS.show()
    //println("==================================")
    //val distincted: Dataset[Emp] = employeDS.distinct()
    //distincted.show()
    //println("==================================")
    //employeDS.dropDuplicates(Array("name")).show()


    spark.stop()
  }

}
