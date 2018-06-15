package com.ljy.redis;

import redis.clients.jedis.Jedis;

public class TestRedis {
    public static void main(String[] args){
        Jedis client = new Jedis("h1",6379);
        String res = client.ping();
        System.out.println(res);

        String sljy = client.set("sljy", "66666");
        System.err.println(sljy);

        String resLjy = client.get(sljy);
        System.out.println(resLjy);
        client.close();
    }
}
