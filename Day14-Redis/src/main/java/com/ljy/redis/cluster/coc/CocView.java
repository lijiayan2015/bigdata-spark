package com.ljy.redis.cluster.coc;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Tuple;

import java.util.HashSet;
import java.util.Set;

public class CocView {
    public static void main(String[] args) throws InterruptedException {
        final HashSet<HostAndPort> hostAndPorts = new HashSet<>();
        hostAndPorts.add(new HostAndPort("h2", 7001));
        hostAndPorts.add(new HostAndPort("h2", 7002));
        hostAndPorts.add(new HostAndPort("h2", 7003));
        hostAndPorts.add(new HostAndPort("h2", 7004));
        hostAndPorts.add(new HostAndPort("h2", 7005));
        hostAndPorts.add(new HostAndPort("h2", 7006));
        JedisCluster cluster = new JedisCluster(hostAndPorts);

        int i = 0;
        while (true) {
            Thread.sleep(2000);
            System.err.println("第" + i + "次查看榜单");
            final Set<Tuple> heros = cluster.zrevrangeWithScores("herosort", 0, 5);
            for (Tuple tuple : heros) {
                final double score = tuple.getScore();
                final String element = tuple.getElement();
                System.err.println(element + "..............." + score);
            }
            i++;
            System.err.println();
        }
    }
}
