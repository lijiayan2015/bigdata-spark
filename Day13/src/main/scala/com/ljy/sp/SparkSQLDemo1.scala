package com.ljy.sp

import com.ljy.scala.LoggerLevels
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQLDemo1 {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val spark = SparkSession.builder()
      .appName("SparkSQLDemo1")
      .master("local[*]")
      .getOrCreate()

    //读取文件,构造一个untype类型的DataFrame
    //Dataframe相当于Dataset[Row]类型
    val df: DataFrame = spark.read.json("hdfs://h1:9000//spark2x/in/day01/people.json")
    df.show()
    df.printSchema()

    //查询 DSL 语言风格
    df.select("name").show()

    df.select("name", "age", "facevalue").filter("age > 20").show()

    import spark.implicits._
    df.select($"name", $"age" + 1, $"facevalue").filter("age > 20").show()
    df.select($"name", $"age" + 1, $"facevalue").filter($"age" > 20).show()

    //SQL语言风格
    df.createOrReplaceTempView("t_person")
    spark.sql("select * from t_person").show()
    spark.stop()

  }
}
