package com.ljy.redis.lol5;

import redis.clients.jedis.Jedis;

import java.util.Random;

public class LolBoxPlayer {
    private static Jedis jedis = new Jedis("h1", 6379);

    public static void main(String[] args) throws Exception {

        Random r = new Random();

        String[] heros = {"蒙多", "提莫", "盖伦", "压缩", "刀妹", "木木", "EZ", "触手怪"};

        while (true) {
            int index = r.nextInt(heros.length);
            String hero = heros[index];
            Thread.sleep(3000);

            //英雄没出现行一次,分数加1,如果初次出场,zincrby方法会自动创建
            jedis.zincrby("hero:ccl", 1, hero);
            System.err.println(hero + "出场了");
        }
    }
}
