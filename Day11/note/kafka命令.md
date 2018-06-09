```scala
查看当前服务器中的所有topic
bin/kafka-topics.sh --list --zookeeper h1:2181

创建topic
bin/kafka-topics.sh --create --zookeeper h1:2181 --replication-factor 1 --partitions 1 --topic test

删除topic
bin/kafka-topics.sh --delete --zookeeper h1:2181 --topic test

需要server.properties中设置delete.topic.enable=true否则只是标记删除或者直接重启。

通过shell命令发送消息
bin/kafka-console-producer.sh --broker-list h1:9092 --topic test

通过shell消费消息
bin/kafka-console-consumer.sh --zookeeper h1:2181 --from-beginning --topic test

查看消费位置
bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker --zookeeper h1:2181 --group testGroup

查看某个Topic的详情
bin/kafka-topics.sh --topic test --describe --zookeeper h1:2181

对分区数进行修改
bin/kafka-topics.sh --zookeeper  h1 --alter --partitions 3 --topic test
```