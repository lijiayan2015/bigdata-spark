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
   - 如果自定义一个类,类里面有很多字段,把这个类当多RDD的排序字段时,如果不做特殊处理,RDD的排序算子就不知道使用哪个字段进行排序了,
     
   - 方式一:
   
   
   
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