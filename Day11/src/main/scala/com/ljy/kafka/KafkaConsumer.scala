package com.ljy.kafka

import java.util.Properties
import java.util.concurrent.{Executor, Executors}

import kafka.consumer.{Consumer, ConsumerConfig, ConsumerIterator, KafkaStream}
import kafka.message.MessageAndMetadata

import scala.collection.mutable

/**
  * 创建Consumer,用于获取Kafka的数据
  */
class KafkaConsumer(val consumer: String, val stream: KafkaStream[Array[Byte], Array[Byte]]) extends Runnable {
  override def run(): Unit = {
    val it: ConsumerIterator[Array[Byte], Array[Byte]] = stream.iterator()
    while (it.hasNext()) {
      val data: MessageAndMetadata[Array[Byte], Array[Byte]] = it.next()
      val topic: String = data.topic
      val partition: Int = data.partition
      val offset = data.offset
      val msg = new String(data.message())
      println(s"topic:$topic,partition:$partition,offset:$offset,msg:$msg")
    }
  }
}


object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    //定义要从哪个topic获取数据
    val topic = "test"

    //定义map,用于存储多个topic
    val topics = new mutable.HashMap[String, Int]()

    //2表示用于读取该topic数据的线程数
    topics.put(topic, 2)

    //配置信息
    val props = new Properties()

    //ConsumerGroupID
    props.put("group.id", "group01")

    //指定zk的列表
    props.put("zookeeper.connect", "h1:2181,h2:2181,h3:2181")

    //指定offset值
    props.put("auto.offset.reset", "smallest")

    //调用Consumer配置类
    val config = new ConsumerConfig(props)

    //创建Consumer实例,该实例在获取数据时,如果没有获取到数据,会一直线程等待
    val consumer = Consumer.create(config)

    //获取数据,map的key代表topic的名称
    //map的value就是获取到的实际数据
    val streams: collection.Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]] = consumer.createMessageStreams(topics)

    val stream: Option[List[KafkaStream[Array[Byte], Array[Byte]]]] = streams.get(topic)

    //创建一个固定大小的线程池
    val poll = Executors.newFixedThreadPool(3)
    for (i <- 0 until stream.size) {
      poll.execute(new KafkaConsumer(s"Consumer:$i",stream.get(i)))
    }
  }
}