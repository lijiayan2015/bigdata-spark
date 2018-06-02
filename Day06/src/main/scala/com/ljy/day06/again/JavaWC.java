package com.ljy.day06.again;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class JavaWC {
    public static void main(String[] args) throws URISyntaxException, IOException {
        Configuration fsconf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://h1:9000"), fsconf);
        Path path = new Path("hdfs://h1:9000/spark/wc/out");
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        SparkConf conf = new SparkConf()
                .setAppName("JavaWC")
                .setMaster("local[*]");

        //创建上下文对象
        JavaSparkContext context = new JavaSparkContext(conf);

        //获取数据
        JavaRDD<String> lines = context.textFile("hdfs://h1:9000/spark/wc");

        //切分数据
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(""));
            }
        });

        //将数据生成元组
        JavaPairRDD<String, Integer> tups = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        //聚合数据
        JavaPairRDD<String, Integer> reduced = tups.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //排序
        JavaPairRDD<Integer, String> forSort = reduced.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> kv) throws Exception {
                return new Tuple2<>(kv._2, kv._1);
            }
        });
        JavaPairRDD<Integer, String> sorted = forSort.sortByKey(true);

        JavaPairRDD<String, Integer> res = sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> vk) throws Exception {
                return new Tuple2<>(vk._2, vk._1);
            }
        });

        System.err.println(res.collect());

        res.saveAsTextFile("hdfs://h1:9000/spark/wc/out/out_2018001-java");

        context.stop();
    }
}

