package com.ljy.redis.cluster;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.List;

/**
 * 向集群中存取list
 */
public class TestRedisClusterList {
    public static void main(String[] args) {
        final HashSet<HostAndPort> hostAndPorts = new HashSet<>();
        hostAndPorts.add(new HostAndPort("h2", 7001));
        hostAndPorts.add(new HostAndPort("h2", 7002));
        hostAndPorts.add(new HostAndPort("h2", 7003));
        hostAndPorts.add(new HostAndPort("h2", 7004));
        hostAndPorts.add(new HostAndPort("h2", 7005));
        hostAndPorts.add(new HostAndPort("h2", 7006));
        JedisCluster cluster = new JedisCluster(hostAndPorts);
        cluster.lpush("list", "hello", "world", "iam", "fine");
        final List<byte[]> lrange = cluster.lrange("list".getBytes(), 0, -1);
        for (byte[] bytes : lrange) {
            System.err.println(new String(bytes));
        }

    }
}
