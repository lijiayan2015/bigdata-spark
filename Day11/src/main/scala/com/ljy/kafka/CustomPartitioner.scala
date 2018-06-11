package com.ljy.kafka

import kafka.producer.Partitioner

class CustomPartitioner(val props:kafka.utils.VerifiableProperties) extends Partitioner {
  override def partition(key: Any, numPartitions: Int): Int = {
    key.hashCode() % numPartitions
  }
}
