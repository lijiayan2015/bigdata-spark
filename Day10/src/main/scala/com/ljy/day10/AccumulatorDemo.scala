package com.ljy.day10

import org.apache.spark.{Accumulator, SparkConf, SparkContext}

/**
  * 累加器
  */
object AccumulatorDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AccumulatorDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val numbers = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9,10), 3)

    val sum: Accumulator[Int] = sc.accumulator(0)
    //var sum = 0

    numbers.foreach(number => {
      sum += number
    })

    println(sum)

  }
}
