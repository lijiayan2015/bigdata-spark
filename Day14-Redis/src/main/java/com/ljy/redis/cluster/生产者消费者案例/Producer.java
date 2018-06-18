package com.ljy.redis.cluster.生产者消费者案例;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Random;
import java.util.UUID;

public class Producer {
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
            int nextInt = random.nextInt(1500);
            try {
                Thread.sleep(nextInt + 1000);
                String taskID = UUID.randomUUID().toString();
                cluster.lpush("{demolist}tasklist", taskID);
                System.err.println("生产了一条消息:" + taskID);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
