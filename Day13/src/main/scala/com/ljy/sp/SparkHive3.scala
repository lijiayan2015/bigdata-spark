package com.ljy.sp

import com.ljy.scala.LoggerLevels
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * spark2x hive 操作
  */
object SparkHive3 {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val spark = SparkSession.builder()
      .appName("SparkHive3")
      .master("local[*]")
      //设置SparkHive的元数据仓库的目录
      .config("spark.sql.warehouse.dir","F:\\work\\java\\IdeaProjects\\BigData-Spark\\Day13\\note\\spark-warehouse")
      //启动Hive支持
      .enableHiveSupport()
      .getOrCreate()



    //创建hive表
    spark.sql("create table if not exists src(key int,value string) row format delimited fields terminated by ' '")
    //导入数据
    // 注意,这里路径如果写成F://将会提示找不到路径,需要使用F:///
    //spark.sql("load data local inpath 'F:///work/java/IdeaProjects/BigData-Spark/Day13/note/emp/hive/hive.txt' overwrite into table src")
    spark.sql("load data local inpath 'F:///work/java/IdeaProjects/BigData-Spark/Day13/note/emp/hive/hive.txt' into table src")

    //查询数据
    spark.sql("select * from src").show()
    spark.sql("select count(*) from src").show()

    println("======================")

    import spark.implicits._
    val hiveDF: DataFrame = spark.sql("select key,value from src where key < 10 order by key desc")

    val hiveDS: Dataset[String] = hiveDF.map {
      case Row(key: Int, value: String) => s"key:$key,value:$value"
    }
    hiveDS.show()

    println("===================")
    //在生成一个DataFrame与src进行表连接
    val recordDF: DataFrame = spark.createDataFrame((1 to 100).map(x=>Record(x,s"val_$x")))
    recordDF.printSchema()
    recordDF.createOrReplaceTempView("t_record")
    spark.sql("select * from t_record join src on t_record.x=src.key").show()
    spark.stop()

  }
  
  
}

case class Record(x:Int,value:String)
