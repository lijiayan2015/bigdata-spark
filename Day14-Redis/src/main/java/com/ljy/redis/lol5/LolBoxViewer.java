package com.ljy.redis.lol5;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.Set;

public class LolBoxViewer {
    private static Jedis jedis = new Jedis("h1", 6379);

    public static void main(String[] args) throws InterruptedException {
        int i = 0;
        while (true) {
            Thread.sleep(3000);
            System.err.println("第" + i + "次查看榜单");
            //从jedis获取榜单信息,获取前5名
            //final Set<Tuple> heros = jedis.zrevrangeByScoreWithScores("hero:ccl", 0, 5);
            Set<Tuple> heros = jedis.zrevrangeWithScores("hero:ccl", 0, 4);
            for (Tuple hero:heros){
                System.err.println(hero.getElement()+"..........."+hero.getScore());
            }

            i++;
            System.err.println();
        }
    }
}
