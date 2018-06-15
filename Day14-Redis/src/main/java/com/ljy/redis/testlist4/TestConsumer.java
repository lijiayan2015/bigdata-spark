package com.ljy.redis.testlist4;

import redis.clients.jedis.Jedis;

import java.util.Random;

public class TestConsumer {

    private static Jedis jedis = new Jedis("h1", 6379);

    public static void main(String[] args) throws Exception {
        Random random = new Random();
        while (true) {
            Thread.sleep(2000);
            //从task-queue1任务队列里面取出一个任务,放到暂存队列里
            String taskID = jedis.rpoplpush("task-queue1", "temp-queue1");

            //模拟消费任务
            if (random.nextInt(17) % 7 == 0) {
                //如果消费失败,要把任务信息从暂存队列里弹出来,并继续放到任务队列里,等待继续被消费
                jedis.rpoplpush("temp-queue1", "task-queue1");
                System.err.println("消费失败:" + taskID);
            } else {
                //如果消费成功,需要把暂存队列里面的任务信息删除
                jedis.rpop("temp-queue1");
                System.out.println("消费成功:" + taskID);
            }
        }
    }
}
