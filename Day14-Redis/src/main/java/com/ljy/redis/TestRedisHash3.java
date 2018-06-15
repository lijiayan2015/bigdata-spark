package com.ljy.redis;

import redis.clients.jedis.Jedis;

import java.util.Map;

public class TestRedisHash3 {
    private static Jedis jedis = new Jedis("h1", 6379);

    /**
     * 添加商品
     */
    private static void addProduct2Cart() {
        jedis.hset("cart:user001", "华为Mate9Pro", "3");
        jedis.hset("cart:user001", "computer", "5");
        jedis.hset("cart:user001", "宝马", "8");
        jedis.hset("cart:user001", "Strem", "3");
        jedis.hset("cart:user001", "拳头大的砖石", "100");
        jedis.close();
    }


    public static void main(String[] args) {
        //addProduct2Cart();
        getProductInfo();
        System.err.println("======================");
        editProInfo();
        System.err.println("======================");
        getProductInfo();
        System.err.println("======================");
        jedis.close();
    }

    /**
     * 获取数据
     */
    private static void getProductInfo() {
        String 华为Mate9Pro = jedis.hget("cart:user001", "华为Mate9Pro");
        final Map<String, String> stringStringMap = jedis.hgetAll("cart:user001");

        System.err.println(华为Mate9Pro);
        System.err.println(stringStringMap);
    }

    //修改信息
    private static void editProInfo() {
        jedis.hset("cart:user001", "Strem", "250");
    }
}
