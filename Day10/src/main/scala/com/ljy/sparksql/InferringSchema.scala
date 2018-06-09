package com.ljy.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object InferringSchema {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("InferringSchema").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //读取数据
    val rdd = sc.textFile("hdfs://h1:9000/spark/sql/in")
    //通过反射推断Schema,将RDD与样例类关联
    val personRDD: RDD[Person] = rdd.map(_.split(",")).map(p => {
      Person(p(0).toInt, p(1), p(2).toInt, p(3).toInt)
    })
    //创建DataFrame
    import sqlContext.implicits._
    val df: DataFrame = personRDD.toDF()
    //查询Schema
    df.printSchema()

    df.registerTempTable("t_person")
    //查询数据
    val res = sqlContext.sql("select * from t_person")
    res.show()

    //将结果写到hdfs
    res.write.mode("append").json("hdfs://h1:9000/spark/sql/out/InferringSchema-2018-06-06")
    sc.stop()


  }
}

//样例类
case class Person(id: Int, name: String, age: Int, face: Int)
