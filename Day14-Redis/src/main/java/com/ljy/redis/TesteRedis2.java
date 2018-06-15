package com.ljy.redis;

import com.google.gson.Gson;
import redis.clients.jedis.Jedis;

import java.io.*;

public class TesteRedis2 {
    private static Jedis jedis = new Jedis("h1", 6379);

    /**
     * 将字符串缓存带String数据结构中
     */
    private static void stringTest() {
        jedis.set("user:01:name", "xiaohong");
        jedis.mset("user:02:name", "zhangsan", "user:03:name", "lisi");

        String uname01 = jedis.get("user:01:name");
        String uname02 = jedis.get("user:02:name");
        String uname03 = jedis.get("user:03:name");

        System.err.println(uname01);
        System.err.println(uname02);
        System.err.println(uname03);
    }

    /**
     * 存字节数组
     */
    private static void objectTest() {
        ProductInfo pinfo = new ProductInfo();
        pinfo.setName("IphoneX");
        pinfo.setPrice(7999.99);
        pinfo.setDesc("买这么贵的手机,到底什么家庭背景");

        // 将对象序列化为byte数组

        try {

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(pinfo);
            //将对象转化为字节数组
            byte[] pBytes = baos.toByteArray();

            //将序列化好的数据缓存到String流中
            jedis.set("product:001".getBytes(),pBytes);

            //读取序列化对象

            byte[] pByteRes = jedis.get("product:001".getBytes());

            ByteArrayInputStream bis = new ByteArrayInputStream(pByteRes);

            ObjectInputStream ois = new ObjectInputStream(bis);

            ProductInfo info = (ProductInfo) ois.readObject();

            System.err.println(info.toString());

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        //stringTest();
        //objectTest();
        objextToJosnTest();
        jedis.close();
    }


    private static final class ProductInfo implements Serializable {
        private String name;
        private double price;
        private String desc;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public double getPrice() {
            return price;
        }

        public void setPrice(double price) {
            this.price = price;
        }

        public String getDesc() {
            return desc;
        }

        public void setDesc(String desc) {
            this.desc = desc;
        }

        @Override
        public String toString() {
            return "ProductInfo{" +
                    "商品名称='" + name + '\'' +
                    ", 商品价格=" + price +
                    ", 商品描述='" + desc + '\'' +
                    '}';
        }
    }

    /**
     * 存json字符串
     */
    public static void objextToJosnTest(){
        ProductInfo productInfo = new ProductInfo();
        productInfo.setName("小米8");
        productInfo.setPrice(4267.66);
        productInfo.setDesc("我也不知道小米的好不好用");

        Gson gson = new Gson();
        String pJson = gson.toJson(productInfo);

        System.err.println(pJson);

        jedis.set("product:002",pJson);

        String pJsonRes = jedis.get("product:002");

        ProductInfo pro = gson.fromJson(pJsonRes, ProductInfo.class);

        System.err.println(pro.toString());

    }
}
