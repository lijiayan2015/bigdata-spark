package com.ljy.day09

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 查询网络访问量最多的省份
  *
  * CREATE TABLE IF NOT EXISTS t_access(
  * id INT PRIMARY KEY AUTO_INCREMENT,
  * province VARCHAR(50) NOT NULL,
  * counts INT DEFAULT 0,
  * access_date DATE
  * )
  * DEFAULT CHARSET utf8;
  */
object MyIPSearch {

  def save2Mysql(tuples: Array[(String, Int)]): Unit = {
    var conn: Connection = null
    var prestat: PreparedStatement = null
    val url = "jdbc:mysql://h1:3306/bigdata?createDatabaseIfNotExist=true&useUnicode=true&characterEncoding=utf8&user=root&password=root"
    val sql = "insert into t_access(province,counts,access_date) values(?,?,?)"
    try {
      conn = DriverManager.getConnection(url)
      tuples.foreach(f => {
        prestat = conn.prepareStatement(sql)
        prestat.setString(1, f._1)
        prestat.setInt(2, f._2)
        prestat.setDate(3, new Date(System.currentTimeMillis()))
        prestat.executeUpdate()

        if (prestat != null) {
          prestat.close()
          prestat = null
        }
      })

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (conn != null) {
        conn.close()
        conn =null
      }
    }

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MyIPSearch").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //读取ip数据
    val ipdata: RDD[String] = sc.textFile("F://work/java/IdeaProjects/BigData-Spark/Day09/note/ip.txt")
    //将ip数据拆分,获取里面的开始ip,结束ip,以及省份
    val ipInfo = ipdata.map(line => {
      val fields = line.split("\\|")
      val startIp = fields(2).toLong
      val endIp = fields(3).toLong
      val province = fields(6)
      (province, startIp, endIp)
    })

    //使用action获取ipinfo的实际数据,
    val arrIp: Array[(String, Long, Long)] = ipInfo.collect()

    //将实际的IP数据广播出去,方便其他节点从本地获取,节省网络io资源
    val broadCastIpInfo: Broadcast[Array[(String, Long, Long)]] = sc.broadcast(arrIp)


    //获取用户访问的数据
    val userAccessInfo = sc.textFile("F://work/java/IdeaProjects/BigData-Spark/Day09/note/http.log")

    //拆封用户访问的数据,得到ip,通过ip查出ip所在的省份
    val provinces: RDD[(String, Int)] = userAccessInfo.map(line => {
      val fields: Array[String] = line.split("\\|")
      val ipAddress = fields(1)
      //将ip转化成long类型的数据
      val ip: Long = ip2Long(ipAddress)
      //根据ip得到所在的省份
      val province = queryProvinceByIp(ip, broadCastIpInfo.value)
      (province, 1)
    })

    val reduced: RDD[(String, Int)] = provinces.reduceByKey(_ + _)

    //将结果存到数据库
    save2Mysql(reduced.collect())

    sc.stop()

  }

  def queryProvinceByIp(ip: Long, arrIp: Array[(String, Long, Long)]): String = {
    var low = 0
    var hight = arrIp.length - 1
    while (low <= hight) {
      val mid = (low + hight) / 2
      if (ip <= arrIp(mid)._3 && ip >= arrIp(mid)._2) {
        return arrIp(mid)._1
      } else if (ip < arrIp(mid)._2) {
        hight = mid - 1
      } else if (ip > arrIp(mid)._3) {
        low = mid + 1
      }
    }
    null
  }


  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }
}
