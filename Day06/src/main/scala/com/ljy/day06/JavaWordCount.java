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
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        //把数据生成元组
        JavaPairRDD<String, Integer> tuples = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        //聚合数据
        JavaPairRDD<String, Integer> reduced = tuples.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //颠倒<key,value>---> <value,key>
        JavaPairRDD<Integer, String> reversed = reduced.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tup) throws Exception {
                return tup.swap();
            }
        }/*new Tuple2<>(keyvalue._2, keyvalue._1)*/);

        //排序,false-->降序
        JavaPairRDD<Integer, String> sorted = reversed.sortByKey(false);
        //反转数据
        JavaPairRDD<String, Integer> res = sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tup) throws Exception {
                return tup.swap();
            }
        });

        System.out.println(res.collect());

        res.saveAsTextFile("hdfs://h1:9000/spark/wc/out/out_20180531-java");
        jsc.stop();

    }
}
