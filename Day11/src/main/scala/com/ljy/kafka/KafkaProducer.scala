package com.ljy.kafka

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

/**
  * 创建一个Producer,用于模拟向Kafka发送数据
  */
object KafkaProducer {

  def main(args: Array[String]): Unit = {
    //定义一个要将数据发送到哪个topic
    val topic = "test"

    //创建一个配置信息类
    val props = new Properties()

    //选择序列化类
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    //指定kafka集群列表
    props.put("metadata.broker.list", "h1:9092,h2:9092,h3:9092")

    //设置发送数据后,响应的方式
    props.put("request.required.acks", "1")

    //调用分区器
    props.put("partitioner.class", "kafka.producer.DefaultPartitioner")

    //实例化producer配置类
    val config = new ProducerConfig(props)

    //创建生产者实例
    val producer: Producer[String, String] = new Producer[String, String](config)

    //模拟生产者发送数据
    for (i <- 1 to 10000) {
      val msg = s"$i:producer send data"
      producer.send(new KeyedMessage[String, String](topic, msg))
    }

  }
}
