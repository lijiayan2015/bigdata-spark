package com.ljy.day10

import java.sql.{Date, DriverManager}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark提供了JDBCRDD,用来获取关系型数据库中的数据,需要指定配置信息
  * 注意:只能获取数据
  * CREATE TABLE IF NOT EXISTS t_access(
  *  id INT PRIMARY KEY AUTO_INCREMENT,
  *  province VARCHAR(50) NOT NULL,
  *  counts INT DEFAULT 0,
  *  access_date DATE
  *  )
  *  DEFAULT CHARSET utf8;
  */
object JDBCRDDDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JDBCRDDDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val jdbcURL = "jdbc:mysql://h1:3306/bigdata?createDatabaseIfNotExist=true&useUnicode=true&characterEncoding=utf8&user=root&password=root"
    val sql = "select id,province,counts,access_date from t_access where id>=? and id<=?"

    val conn = () =>{
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection(jdbcURL)
    }


    val jdbcRDD: JdbcRDD[(Int, String, Int, Date)] = new JdbcRDD(
      sc, conn, sql, 0, 100, 1, res => {
        val id = res.getInt("id")
        val province = res.getString("province")
        val counts = res.getInt("counts")
        val date = res.getDate("access_date")
        (id, province, counts, date)
      }
    )
    jdbcRDD.foreach(println)
  }
}
