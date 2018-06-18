package com.ljy.redis.cluster;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

/**
 * 向集群中存取set
 */
public class TestRedisClusterSet {
    public static void main(String[] args) {
        final HashSet<HostAndPort> hostAndPorts = new HashSet<>();
        hostAndPorts.add(new HostAndPort("h2", 7001));
        hostAndPorts.add(new HostAndPort("h2", 7002));
        hostAndPorts.add(new HostAndPort("h2", 7003));
        hostAndPorts.add(new HostAndPort("h2", 7004));
        hostAndPorts.add(new HostAndPort("h2", 7005));
        hostAndPorts.add(new HostAndPort("h2", 7006));
        JedisCluster cluster = new JedisCluster(hostAndPorts);
        final Long sadd = cluster.sadd("618order", "小红花", "小红花","大红花", "充电宝宝", "xxx");
        System.err.println("添加结果:"+sadd);
        final Set<String> smembers = cluster.smembers("618order");
        for (String str:smembers){
            System.err.println(str);
        }
    }
}
