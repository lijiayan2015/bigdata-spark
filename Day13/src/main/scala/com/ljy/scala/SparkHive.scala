package com.ljy.scala

import org.apache.spark.sql.SparkSession

object SparkHive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("")
      .master("local[2]")
      //设置Spark sql的元数据仓库的目录
      .config("spark.sql.warehouse.dir", "F://work/java/IdeaProjects/BigData-Spark/Day13/note/spark-warehouse")
      //启动hive支持
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    //创建hive表,并load数据,最后查询数据
    spark.sql("create table if not exists src(key int,value string) row format delimited fields terminated by ' '")
    spark.sql("load data local inpath 'C:\\Users\\lijia\\Desktop\\data\\hive.txt' into table src")

    spark.sql("select * from src").show()
    spark.sql("select count(*) from src").show()


    spark.stop()
  }
}
