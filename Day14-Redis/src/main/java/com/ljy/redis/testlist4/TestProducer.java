package com.ljy.redis.testlist4;

import redis.clients.jedis.Jedis;

import java.util.Random;
import java.util.UUID;

public class TestProducer {
    private static Jedis jedis = new Jedis("h1", 6379);

    public static void main(String[] args) throws Exception {
        Random random = new Random();
        while (true) {
            int next = random.nextInt(1000);
            Thread.sleep(1500 + next);
            String taskID = UUID.randomUUID().toString();
            jedis.lpush("task-queue1", taskID);
            System.err.println("生成一条任务信息:" + taskID);
        }
    }
}
