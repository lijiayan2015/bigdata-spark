package com.ljy.redis.cluster;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;

/**
 * 向集群中存取字符串
 */
public class TestRedisClusterString {
    public static void main(String[] args){
        final HashSet<HostAndPort> hostAndPorts = new HashSet<>();
        hostAndPorts.add(new HostAndPort("h2",7001));
        hostAndPorts.add(new HostAndPort("h2",7002));
        hostAndPorts.add(new HostAndPort("h2",7003));
        hostAndPorts.add(new HostAndPort("h2",7004));
        hostAndPorts.add(new HostAndPort("h2",7005));
        hostAndPorts.add(new HostAndPort("h2",7006));
        JedisCluster cluster = new JedisCluster(hostAndPorts);

        final String str1 = cluster.set("key1", "hello");
        final String str11 = cluster.get("key1");
        System.err.println("存:"+str1);
        System.err.println("取:"+str11);
    }
}
