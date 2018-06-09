package com.ljy.day10

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSQLTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSQLTest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlSc = new SQLContext(sc)

    val seq = Seq(("1", "jerry", 4), ("2", "suke", 6), ("3", "tom", 5))
    val rdd1 = sc.parallelize(seq)

    import sqlSc.implicits._

    val df = rdd1.toDF("id", "name", "age")

    df.select("name", "age").filter("age >5").show
    sc.stop()
  }
}
