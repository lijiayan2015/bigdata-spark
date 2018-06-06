### Checkpoint(检查点)

   - 在发生shuffle后,往往中间结果数据很重要,如果丢失的话,
      需要再次进行计算,这样会消耗相当多的物理资源和时间
      为了保证数据的安全性,需要做Checkpoint.
      
   - 注意:
   
        - 最好把数据checkpoint到hdfs,这样便于集群中的所有节点都能获取到,
          而且HDFS的多副本机制也保证了数据的安全性
          
        - 在checkpoint之前最好先cache一下,这样便于运行任务的时候快速的用到.
          也便于在Checkpoint的时候直接从内存获取数据,提高获取数据的速度.
   
   - 实现步骤
        - 设置Checkpoint的目录
            ```scala
              sc.setCheckpointDir("hdfs://h1:9000/spark/chechpoint")
            ```
        - 把shuffle后的数据进行缓存
            ```scala
              val cached = rdd1.cache()
            ```
        - 把缓存后的数据做checkpoint
            ```scala
              cached.checkpoint
            ```
        - 查看数据是否做了检查点:
            ```scala
              cached.isCheckpointed
            ```
        - 查看Checkpoint的文件目录信息
            ```scala
              rdd1.getCheckpointFile
            ```