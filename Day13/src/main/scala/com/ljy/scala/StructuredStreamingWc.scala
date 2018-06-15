package com.ljy.scala

import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, SparkSession}

object StructuredStreamingWc {
  def main(args: Array[String]): Unit = {

    LoggerLevels.setStreamingLogLevels()
    val spark = SparkSession
      .builder()
      .appName("BasicOpration")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.readStream.format("socket").option("host", "h1").option("port", "6666").load()
    val ds: Dataset[String] =df.as[String]
    val splited: Dataset[String] = ds.flatMap(_.split(" "))
    val grouped: RelationalGroupedDataset = splited.groupBy("value")
    val res: DataFrame = grouped.count()
    val query = res.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()

  }
}
