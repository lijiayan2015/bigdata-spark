package com.ljy.redis.cluster.生产者消费者案例;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Random;

public class Consumer {
    public static void main(String[] args) {
        final HashSet<HostAndPort> hostAndPorts = new HashSet<>();
        hostAndPorts.add(new HostAndPort("h2", 7001));
        hostAndPorts.add(new HostAndPort("h2", 7002));
        hostAndPorts.add(new HostAndPort("h2", 7003));
        hostAndPorts.add(new HostAndPort("h2", 7004));
        hostAndPorts.add(new HostAndPort("h2", 7005));
        hostAndPorts.add(new HostAndPort("h2", 7006));
        JedisCluster cluster = new JedisCluster(hostAndPorts);
        Random random = new Random();
        while (true) {
            try {
                Thread.sleep(2000);
                //从任务栈里面获取一条任务,放到存队列里面
                final String taskID = cluster.rpoplpush("{demolist}tasklist", "{demolist}temptasklist");
                //模拟消费任务
                if (random.nextInt(17) % 7 == 0) {
                    //消费失败,取出来继续放到任务队列里面
                    cluster.rpoplpush("{demolist}temptasklist", "{demolist}tasklist");
                    System.err.println("消费失败:" + taskID);
                } else {
                    //消费成功,从临时队列中取出来
                    cluster.rpop("{demolist}temptasklist");
                    System.err.println("消费成功一条数据:" + taskID);
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
