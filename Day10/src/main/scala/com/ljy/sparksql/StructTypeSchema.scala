package com.ljy.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object StructTypeSchema {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StructTypeSchema").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //读取数据
    val rdd = sc.textFile("hdfs://h1:9000/spark/sql/in")

    val personRDD = rdd.map(line => line.split(","))

    //通过StructType指定Schema
    val schema = StructType {
      Array(
        StructField("id", IntegerType, false),
        StructField("name", StringType, false),
        StructField("age", IntegerType, false),
        StructField("face", IntegerType, false)
      )
    }

    //将RDD映射到rowRDD
    val rowRDD: RDD[Row] = personRDD.map(p => {
      Row(p(0).toInt, p(1), p(2).toInt, p(3).toInt)
    })

    //将Schema信息应用到rowRDD
    val df = sqlContext.createDataFrame(rowRDD, schema)

    //注册临时表
    df.registerTempTable("t_person")

    //查询数据
    val res = sqlContext.sql("select * from t_person")

    //将数据存储到hdfs
    res.write.mode("append").json("hdfs://h1:9000/spark/sql/out/StructTypeSchema-1028-0607")

    res.show
    sc.stop()

  }
}
