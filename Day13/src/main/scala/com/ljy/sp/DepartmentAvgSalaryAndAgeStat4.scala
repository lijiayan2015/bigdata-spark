package com.ljy.sp

import com.ljy.scala.LoggerLevels
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 计算部门的平均薪资和年龄
  * 需求:
  *   1.只统计年龄在20岁以上的员工
  *   2.根据部门名称和员工性别来为粒度来进行统计
  *   3.统计每个部门分性别的平均薪资和平均年龄
  */
object DepartmentAvgSalaryAndAgeStat4 {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("DepartmentAvgSalaryAndAgeStat4")
      .getOrCreate()

    import spark.implicits._

    val employee: DataFrame = spark.read.json("F:\\work\\java\\IdeaProjects\\BigData-Spark\\Day13\\note\\emp\\employee.json")
    val department: DataFrame = spark.read.json("F:\\work\\java\\IdeaProjects\\BigData-Spark\\Day13\\note\\emp\\department.json")

    //导入sql函数
    import org.apache.spark.sql.functions._
    // 进行操作
    employee
      //过滤出20岁以上的员工
      .filter("age > 20")
      // 根据部门名称和员工性别进行聚合,需要和department进行join
      .join(department, $"depId" === $"id")
      //根据员工部门和性别进行分组
      .groupBy(department("name"), employee("gender"))
      //进行聚合
      .agg(avg(employee("salary")), avg(employee("age")),count(employee("name")))
      .show()
    spark.stop()


  }
}
