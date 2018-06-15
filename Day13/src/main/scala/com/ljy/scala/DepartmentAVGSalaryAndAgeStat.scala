package com.ljy.scala

import org.apache.spark.sql.SparkSession

object DepartmentAVGSalaryAndAgeStat {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("")
      .master("local[2]")
      //设置Spark sql的元数据仓库的目录
      .config("spark.sql.warehouse.dir", "F://work/java/IdeaProjects/BigData-Spark/Day13/note/spark-warehouse")
      //启动hive支持
      //.enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val employee = spark.read.json("F:\\work\\java\\IdeaProjects\\BigData-Spark\\Day13\\note\\emp\\employee.json")

    val department = spark.read.json("F:\\work\\java\\IdeaProjects\\BigData-Spark\\Day13\\note\\emp\\employee2.json")

    import org.apache.spark.sql.functions._

    employee.filter("age > 20")
      .join(department,$"depid"=== $"id")
      .groupBy(department("name"),employee("gender"))
      .agg(avg(employee("salary")),avg(employee("age")))

    spark.stop()
  }
}
