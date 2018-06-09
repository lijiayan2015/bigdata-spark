package com.ljy.day10

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 通过反射推断Schema
  */
object InferSchema {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("InferSchema").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlSc = new SQLContext(sc)

    //获取数据
    val lineRDD = sc.textFile("hdfs://h1:9000/spark/sql/in")
    //切分数据
    val splited: RDD[Array[String]] = lineRDD.map(_.split(","))

    // 将RDD和Person样例类进行关联
    val personRDD = splited.map(p => {
      Person((p(0).toInt), p(1), p(2).toInt, p(3).toInt)
    })

    // 调用toDF方法,需要隐式转换的函数
    import sqlSc.implicits._

    //将personRDD转黄成DataFrame
    val df: DataFrame = personRDD.toDF("id", "name", "age", "face")

    //注册成为一张临时表
    df.registerTempTable("t_person")

    val sql = "select * from t_person where face >= 80 order by age desc limit 10"
    val res = sqlSc.sql(sql)
    res.write.mode("append").json("hdfs://h1:9000/spark/sql/out/out-2018-0606")

    sc.stop()
  }
}

case class Person(id: Int, name: String, age: Int, face: Int)