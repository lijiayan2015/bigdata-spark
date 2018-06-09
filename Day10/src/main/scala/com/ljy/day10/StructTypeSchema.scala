package com.ljy.day10

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object StructTypeSchema {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("InferSchema").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlSc = new SQLContext(sc)

    //获取数据
    val lineRDD = sc.textFile("hdfs://h1:9000/spark/sql/in")
    //切分数据
    val splited: RDD[Array[String]] = lineRDD.map(_.split(","))

    // StrcuType指定Schema
    val schema: StructType = StructType {
      Array(
        StructField("id", IntegerType, false),
        StructField("name", StringType, true),
        StructField("age", IntegerType, false),
        StructField("face", IntegerType, false)

      )
    }
    // 开始映射
    val rowRdd: RDD[Row] = splited.map(p=>Row(p(0).toInt,p(1),p(2).toInt,p(3).toInt))

    //转化我dataframe
    val df: DataFrame = sqlSc.createDataFrame(rowRdd,schema)

    //注册临时表
    df.registerTempTable("t_person")

    val sql = "select * from t_person"

    val res = sqlSc.sql(sql)
    res.write.mode("append").json("F:\\work\\java\\IdeaProjects\\BigData-Spark\\Day10\\note\\res/out001");


  }
}
