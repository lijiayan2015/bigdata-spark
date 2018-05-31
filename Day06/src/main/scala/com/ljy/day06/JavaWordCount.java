package com.ljy.day06;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class JavaWordCount {
    public static void main(String[] args) {


        //创建配置信息类
        /*模板代码*/
        SparkConf conf = new SparkConf()
                .setAppName("JavaWC");
                //.setMaster("local[*]");

        //上传上下文对象
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //获取数据
        JavaRDD<String> lines = jsc.textFile("hdfs://h1:9000/spark/wc");

        //切分数据
        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")));

        //把数据生成元组
        JavaPairRDD<String, Integer> tuples = words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));

        //聚合数据
        JavaPairRDD<String, Integer> reduced = tuples.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        //颠倒<key,value>---> <value,key>
        JavaPairRDD<Integer, String> reversed = reduced.mapToPair((PairFunction<Tuple2<String, Integer>, Integer, String>) Tuple2::swap/*new Tuple2<>(keyvalue._2, keyvalue._1)*/);

        //排序,false-->降序
        JavaPairRDD<Integer, String> sorted = reversed.sortByKey(false);
        //反转数据
        JavaPairRDD<String, Integer> res = sorted.mapToPair((PairFunction<Tuple2<Integer, String>, String, Integer>) Tuple2::swap);

        System.out.println(res.collect());

        res.saveAsTextFile("hdfs://h1:9000/spark/wc/out/out_20180531-java");
        jsc.stop();

    }
}
