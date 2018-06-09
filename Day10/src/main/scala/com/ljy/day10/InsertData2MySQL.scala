package com.ljy.day10

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * 用sparksql将数据写入到mysql中
  */
object InsertData2MySQL {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("InsertData2MySQL")/*.setMaster("local[*]")*/
    val sc = new SparkContext(conf)
    val sqlSc = new SQLContext(sc)

    //获取数据
    val lineRDD = sc.textFile("hdfs://h1:9000/spark/sql/in")
    //切分数据
    val splited: RDD[Array[String]] = lineRDD.map(_.split(","))



    val schema = StructType{
      Array(
        StructField("name",StringType,false),
        StructField("age",IntegerType,false),
        StructField("face",IntegerType,false)
      )
    }

    val personRDD = splited.map(p=>{Row(p(1),p(2).toInt,p(3).toInt)})
    val df = sqlSc.createDataFrame(personRDD,schema)

    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","root")
    prop.put("driver","com.mysql.jdbc.Driver")

    val url = "jdbc:mysql://h1:3306/bigdata"
    val table = "person"

    df.write.mode("append").jdbc(url,table,prop)

    sc.stop()

  }

}
