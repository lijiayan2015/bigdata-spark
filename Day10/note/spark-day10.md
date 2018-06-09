### JDBCRDD
  - Demo代码:
    ```scala
        package com.ljy.day10
        
        import java.sql.{Date, DriverManager}
        
        import org.apache.spark.rdd.JdbcRDD
        import org.apache.spark.{SparkConf, SparkContext}
        
        /**
          * Spark提供了JDBCRDD,用来获取关系型数据库中的数据,需要指定配置信息
          * 注意:只能获取数据
          * CREATE TABLE IF NOT EXISTS t_access(
          *  id INT PRIMARY KEY AUTO_INCREMENT,
          *  province VARCHAR(50) NOT NULL,
          *  counts INT DEFAULT 0,
          *  access_date DATE
          *  )
          *  DEFAULT CHARSET utf8;
          */
        object JDBCRDDDemo {
          def main(args: Array[String]): Unit = {
            val conf = new SparkConf().setAppName("JDBCRDDDemo").setMaster("local[*]")
            val sc = new SparkContext(conf)
        
            val jdbcURL = "jdbc:mysql://h1:3306/bigdata?createDatabaseIfNotExist=true&useUnicode=true&characterEncoding=utf8&user=root&password=root"
            val sql = "select id,province,counts,access_date from t_access where id>=? and id<=?"
        
            val conn = () =>{
              Class.forName("com.mysql.jdbc.Driver").newInstance()
              DriverManager.getConnection(jdbcURL)
            }
        
        
            val jdbcRDD: JdbcRDD[(Int, String, Int, Date)] = new JdbcRDD(
              sc, conn, sql, 0, 100, 1, res => {
                val id = res.getInt("id")
                val province = res.getString("province")
                val counts = res.getInt("counts")
                val date = res.getDate("access_date")
                (id, province, counts, date)
              }
            )
            jdbcRDD.foreach(println)
          }
        }

    ```
    
### 自定义排序(二次排序)
   - 如果自定义一个类，类里有多个字段，现在要对某些字段进行排序，
     如果直接调用SortBy算子来排序，或者要根据两个字段来排序,就会出错，因为该算子不会知道到底以哪个字段进行排序,
     Spark提供了自定义排序功能
     
   - 方式一:
        ```scala
         package com.ljy.day10
         
         object MyPref {
           //第一种排序方式
           implicit val girlOrdering = new Ordering[Girl] {
             override def compare(x: Girl, y: Girl): Int = {
               if (x.face==y.face){
                 x.age-y.age
               }else{
                 y.face - x.face
               }
             }
           }
         }
    
        ``` 
        ```scala
         package com.ljy.day10
         
         import org.apache.spark.{SparkConf, SparkContext}
         
         object CustomSortDemo {
           def main(args: Array[String]): Unit = {
         
             val conf = new SparkConf().setAppName("CustomSortDemo").setMaster("local[*]")
             val sc = new SparkContext(conf)
         
             val girlInfo = sc.parallelize(List(("mimi", 80, 35), ("bb", 90, 37), ("hh", 86, 32)))
         
             //第一种排序方式
             /*val sorted: RDD[(String, Int, Int)] = girlInfo.sortBy(_._2)
             println(sorted.collect().reverse.toBuffer)*/
         
             import MyPref.girlOrdering
             val sorted = girlInfo.sortBy(goddss => Girl(goddss._1, goddss._2, goddss._3), false)
         
             println(sorted.collect().toBuffer)
             
             sc.stop()
           }
         }
         
         
         case class Girl(name: String, face: Int, age: Int)
        
        ```  
   - 方式二:
        ```scala
             package com.ljy.day10
             
             import org.apache.spark.{SparkConf, SparkContext}
             
             object CustomSortDemo {
               def main(args: Array[String]): Unit = {
             
                 val conf = new SparkConf().setAppName("CustomSortDemo").setMaster("local[*]")
                 val sc = new SparkContext(conf)
             
                 val girlInfo = sc.parallelize(List(("mimi", 80, 35), ("bb", 90, 37), ("hh", 86, 32)))
             
                 //第一种排序方式
                 /*val sorted: RDD[(String, Int, Int)] = girlInfo.sortBy(_._2)
                 println(sorted.collect().reverse.toBuffer)*/
                 
                 val sorted2 = girlInfo.sortBy(goddss => MyGirl(goddss._1, goddss._2, goddss._3), false)
                 println(sorted2.collect().toBuffer)
             
                 sc.stop()
               }
             }

        
           
            case class MyGirl(name: String, face: Int, age: Int) extends Ordered[MyGirl] {
              override def compare(that: MyGirl): Int = {
                if (this.face == that.face) {
                  this.age - that.age
                } else {
                  this.face - that.face
                }
              }
            }

        ```
   
   
   
   
### Accumulator(累加器):

   - 提供的Accumulator累加器,用于多个Executor并发的对一个变量进行累加操作
     其实就是多个task对一个变量发行操作的过程,task只能对Accumulator累加器
     累加的操作,不能读取值,只有Driver端才能读取<br/>
       ```scala
       package com.ljy.day10
       
       import org.apache.spark.{Accumulator, SparkConf, SparkContext}
       
       /**
         * 累加器
         */
       object AccumulatorDemo {
         def main(args: Array[String]): Unit = {
           val conf = new SparkConf().setAppName("AccumulatorDemo").setMaster("local[*]")
           val sc = new SparkContext(conf)
       
           val numbers = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9,10), 3)
       
           val sum: Accumulator[Int] = sc.accumulator(0)
           //var sum = 0
       
           numbers.foreach(number => {
             sum += number
           })
       
           println(sum)
       
         }
       }

       ```
### SparkSQL
   - Spark SQL是Spark用来处理结构化数据的一个模块，它提供了一个编程抽象叫做DataFrame并且作为分布式SQL查询引擎的作用。
   - Spark SQL的特性:
        - 易整合
        - 统一的数据访问方式
        - 兼容Hive
        - 标准的数据连接
   - DataFrames <br/>
        与RDD类似，DataFrame也是一个分布式数据容器。然而DataFrame更像传统数据库的二维表格，
        除了数据以外，还记录数据的结构信息，即schema。同时，与Hive类似，
        DataFrame也支持嵌套数据类型（struct、array和map）。从API易用性的角度上看，
        DataFrame API提供的是一套高层的关系操作，比函数式的RDD API要更加友好，门槛更低。
        由于与R和Pandas的DataFrame类似，Spark DataFrame很好地继承了传统单机数据分析的开发体验。

   - DSL语言风格
        ```scala
        scala> val seq = Seq(("1","jerry",4),("2","suke",6),("3","tom",5))
        seq: Seq[(String, String, Int)] = List((1,jerry,4), (2,suke,6), (3,tom,5))
        
        scala> val rdd1 = sc.parallelize(seq)
        rdd1: org.apache.spark.rdd.RDD[(String, String, Int)] = ParallelCollectionRDD[0] at parallelize at <console>:29
        
        //将RDD转换成dataframe
        scala> val df = rdd1.toDF("id","name","age")
        df: org.apache.spark.sql.DataFrame = [id: string, name: string, age: int]
        
        scala> df.show()
        +---+-----+---+
        | id| name|age|
        +---+-----+---+
        |  1|jerry|  4|
        |  2| suke|  6|
        |  3|  tom|  5|
        +---+-----+---+
        
        
        scala> df.select("name").show
        +-----+
        | name|
        +-----+
        |jerry|
        | suke|
        |  tom|
        +-----+
   
        scala> df.select("name","age").filter(col("age")>5).show
        +----+---+
        |name|age|
        +----+---+
        |suke|  6|
        +----+---+
         //注意,如果是df.select("name").filter(col("age")>5).show 这样的将会报错,原因是filter是在select出结果后再过滤的.
    
        scala> df.select("name","age").filter("age>5").show
        +----+---+
        |name|age|
        +----+---+
        |suke|  6|
        +----+---+
        
        ================================
        scala> val df = List((1,"tom",3),(2,"jerry",4)).toDF("id","name","age")
        df: org.apache.spark.sql.DataFrame = [id: int, name: string, age: int]
        
        scala> df.select("*").show
        +---+-----+---+
        | id| name|age|
        +---+-----+---+
        |  1|  tom|  3|
        |  2|jerry|  4|
        +---+-----+---+
   
        ```
        
   - SQL语句的风格
   
        ```scala
        scala> val seq = Seq(("1","jerry",4),("2","suke",6),("3","tom",5))
        seq: Seq[(String, String, Int)] = List((1,jerry,4), (2,suke,6), (3,tom,5))
        
        scala> val rdd1 = sc.parallelize(seq)
        rdd1: org.apache.spark.rdd.RDD[(String, String, Int)] = ParallelCollectionRDD[0] at parallelize at <console>:29
        
        //将RDD转换成dataframe
        scala> val df = rdd1.toDF("id","name","age")
   
        //指定临时表
        scala> df.registerTempTable("t_person")
        scala> sqlContext.sql("select id,name,age from t_person where age >=5 limit 10").show
        +---+----+---+
        | id|name|age|
        +---+----+---+
        |  2|suke|  6|
        |  3| tom|  5|
        +---+----+---+
        //注意:用sql形式的语句并不会像DSL语句那样,条件中用到的字段必须在select中存在.
        //sqlContext.sql("select id,name from t_user where age>18") 这样写并不会报错.
   
        ```
   
   - 样例类进行字段映射
        ```scala
           vim person.txt
           1,张三,20,90
           2,李四,30,86
           3,王五,30,96
           4,赵六,24,87
           val rdd = sc.textFile("hdfs://h1:9000/spark/sql/in")
           
           case class Person(id:Long,name:String,age:Int,face:Int);
           val personRdd = rdd.map(_.split(",")).map(p=>Person(p(0).toLong,p(1),p(2).toInt,p(3).toInt))
           val personDF = personRdd.toDF
           personDF.show
           scala> personDF.show
           +---+--------+---+----+
           | id|    name|age|face|
           +---+--------+---+----+
           |  1|zhangsan| 30|  80|
           |  2|    lisi| 24|  99|
           |  3|  wangwu| 18|  98|
           |  4| zhaoliu| 19|  89|
           |  5|  zhouqi| 20|  88|
           +---+--------+---+----+
           
           personDF.select("name","face").show
           //查询所有的name,age,并将age+1
           scala> personDF.select(col("name"),col("age")+1).show
           +--------+---------+
           |    name|(age + 1)|
           +--------+---------+
           |zhangsan|       31|
           |    lisi|       25|
           |  wangwu|       19|
           | zhaoliu|       20|
           |  zhouqi|       21|
           +--------+---------+
           
           scala> personDF.select(personDF("id"),personDF("name"),personDF("age")+1,personDF("face")).show
           +---+--------+---------+----+
           | id|    name|(age + 1)|face|
           +---+--------+---------+----+
           |  1|zhangsan|       31|  80|
           |  2|    lisi|       25|  99|
           |  3|  wangwu|       19|  98|
           |  4| zhaoliu|       20|  89|
           |  5|  zhouqi|       21|  88|
           +---+--------+---------+----+
      
            //按年龄进行分组,并统计相同年龄的人数
            personDF.groupBy("age").count.show
            //或者
             personDF.groupBy(personDF("age")).count.show
            +---+-----+
            |age|count|
            +---+-----+
            | 18|    1|
            | 19|    1|
            | 20|    1|
            | 24|    1|
            | 30|    1|
            +---+-----+
           
           //生成临时表
           person.registerTempTable("t_person")
           
           sqlContext.sql("select * from t_person").show
           
           //查看DataFrom的Schema信息
           personDF.printSchema
           scala> personDF.printSchema
           root
           |-- id: integer (nullable = false)
           |-- name: string (nullable = true)
           |-- age: integer (nullable = false)
           |-- face: integer (nullable = false)
           //查看表的信息
           sqlContext.sql("desc t_person").show
           +--------+---------+-------+
           |col_name|data_type|comment|
           +--------+---------+-------+
           |      id|      int|       |
           |    name|   string|       |
           |     age|      int|       |
           |    face|      int|       |
           +--------+---------+-------+
        ```
        
        
### 编程方式执行Spark SQL查询

   - 添加Spark SQL的maven依赖
       ```scala
          <dependency>
              <groupId>org.apache.spark</groupId>
              <artifactId>spark-sql_2.10</artifactId>
              <version>1.6.3</version>
          </dependency>
       ```
            
   - 通过反射方式推断Schema,代码如下:
       ```scala
         package com.ljy.sparksql
         
         import org.apache.spark.rdd.RDD
         import org.apache.spark.sql.{DataFrame, SQLContext}
         import org.apache.spark.{SparkConf, SparkContext}
         
         object InferringSchema {
           def main(args: Array[String]): Unit = {
             val conf = new SparkConf().setAppName("InferringSchema").setMaster("local[*]")
             val sc = new SparkContext(conf)
             val sqlContext = new SQLContext(sc)
         
             //读取数据
             val rdd = sc.textFile("hdfs://h1:9000/spark/sql/in")
             //通过反射推断Schema,将RDD与样例类关联
             val personRDD: RDD[Person] = rdd.map(_.split(",")).map(p => {
               Person(p(0).toInt, p(1), p(2).toInt, p(3).toInt)
             })
             //创建DataFrame
             import sqlContext.implicits._
             val df: DataFrame = personRDD.toDF()
             //查询Schema
             df.printSchema()
         
             df.registerTempTable("t_person")
             //查询数据
             val res = sqlContext.sql("select * from t_person")
             res.show()
             
             //将结果写到hdfs
             res.write.mode("append").json("hdfs://h1:9000/spark/sql/out/InferringSchema-2018-06-06")
             sc.stop()
             
             
           }
         }
         
         //样例类
         case class Person(id: Int, name: String, age: Int, face: Int)

       ```
        - 将上面代码打成jar包提交到spark集群,运行时jar时的脚本:
           
           ```scala
             //先将conf的setMaster注释掉
             
             $SPARK_HOME/bin/spark-submit \
             --class com.ljy.sparksql.InferringSchema \
             --master "spark://h1:7077" \
             --executor-memory 512m \
             --total-executor-cores 2 \
             Day10-1.0.jar
                   
           ```
           
   - 通过StructType指定Schema,代码如下:
        ```scala
           
        ```