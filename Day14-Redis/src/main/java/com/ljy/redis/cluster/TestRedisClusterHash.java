package com.ljy.redis.cluster;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.*;

/**
 * 向集群中存取hashmap
 */
public class TestRedisClusterHash {
    public static void main(String[] args) {
        final HashSet<HostAndPort> hostAndPorts = new HashSet<>();
        hostAndPorts.add(new HostAndPort("h2", 7001));
        hostAndPorts.add(new HostAndPort("h2", 7002));
        hostAndPorts.add(new HostAndPort("h2", 7003));
        hostAndPorts.add(new HostAndPort("h2", 7004));
        hostAndPorts.add(new HostAndPort("h2", 7005));
        hostAndPorts.add(new HostAndPort("h2", 7006));
        JedisCluster cluster = new JedisCluster(hostAndPorts);
        cluster.hset("618","充气娃娃","1000元");
        cluster.hset("618","金瓶梅","58元");
        cluster.hset("618","小电影","128部");

        final String 充气娃娃 = cluster.hget("618", "充气娃娃");

        System.err.println(充气娃娃);
        final Map<String, String> map = cluster.hgetAll("618");

        final Set<Map.Entry<String, String>> entries = map.entrySet();
        final Iterator<Map.Entry<String, String>> it = entries.iterator();
        while (it.hasNext()){
            final Map.Entry<String, String> next = it.next();
            final String key = next.getKey();
            final String value = next.getValue();
            System.out.println(key+":"+value);
        }
    }
}
