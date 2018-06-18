package com.ljy.redis.cluster.coc;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Random;

public class CocPlayer {

    public static void main(String[] args) throws InterruptedException {
        final HashSet<HostAndPort> hostAndPorts = new HashSet<>();
        hostAndPorts.add(new HostAndPort("h2", 7001));
        hostAndPorts.add(new HostAndPort("h2", 7002));
        hostAndPorts.add(new HostAndPort("h2", 7003));
        hostAndPorts.add(new HostAndPort("h2", 7004));
        hostAndPorts.add(new HostAndPort("h2", 7005));
        hostAndPorts.add(new HostAndPort("h2", 7006));
        JedisCluster cluster = new JedisCluster(hostAndPorts);

        Random random = new Random();
        final String[] heros = {"女王", "男王", "野蛮人", "野猪", "弓箭手", "法师", "飞龙", "飞龙宝宝", "炸弹人", "肉胖"};
        while (true) {
            int index = random.nextInt(heros.length);
            final String hero = heros[index];
            Thread.sleep(3000);
            cluster.zincrby("herosort",1,hero);
            System.err.println(hero+" 出场了");
        }
    }
}
