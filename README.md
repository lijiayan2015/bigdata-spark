### bigdata-spark

### 目录
   - [Day06](./Day06)
        - [笔记](./Day06/note/spark-day06.md)
        - 主要内容:
            1. Spark环境搭建
            2. Spark案例测试
            3. 遇到的问题以及解决方案
            4. 高可用Spark集群搭建
   
   
   - [Day07](./Day07)
        - [笔记](./Day07/note/day07-note.md)
        - 主要内容:
            1. Spark Core的常用算子RDD概述以及使用案例

    
   - [Day08](./Day08)
        - [笔记](./Day08/note/spark-day08-note.md)
        - 主要内容:
            1. Spark启动流程
            2. Spark提交任务的流程
            3. RDD依赖的两种关系
            4. Lineage
            5. RDD的缓存
            6. DAG以及State
   
   - [Day09](./Day09)
        - [笔记](./Day09/note/spark-day-09.md)
        - 主要内容:
            1. Checkpoint(检查点)
            2. 与JDBC的有关操作:[代码](./Day09/src/main/scala/com/ljy/day09/MyIPSearch.scala)
            3. 自定义分区器解决数据倾斜的问题:[代码](./Day09/src/main/scala/com/ljy/day09/MySubjectCount.scala)

   - [Day10](./Day10)
        - [笔记](./Day10/note/spark-day10.md)
        - 主要内容:
            1. JDBCRDD
            2. 自定义排序(二次排序)
            3. Accumulator(累加器):
            4. SparkSQL
            5. 编程方式执行Spark SQL查询
            6. Hive(待完成)
            
   - [Day11](./Day11)
        - [笔记](./Day11/note/spark-day11.md)
        - 主要内容:
            1. kafka的原理
            2. kafka集群配置
            3. [kafka的命令](./Day11/note/kafka命令.md)
            4. [kafka-manager](./Day11/note/kafka-manager/Kafka-Manager.md)
            
   - [Day12](./Day12)
        - [笔记](./Day12/note/spark-day12.md)
        - 主要内容:
            1. spark-streaming简介
            2. DStream简介
            3. DStream原语
            4. UpdateStateByKey
            5. Transform 
            6. SparkStreaming获取kafka的数据
            7. Window 
            8. Spark-On-Yarn概述
            9. Spark-On-Yarn 环境配置
            10. 运行模式(cluster模式和client模式)