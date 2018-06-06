package com.ljy.day09

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IPSearch {


  def main(args: Array[String]): Unit = {
    /*val conf = new SparkConf().setAppName("IPSearch").setMaster("local")
    val sc = new SparkContext(conf)

    //获取IP段区域分布的基础数据
    val ipinfo = sc.textFile("")

    val splitedIPInfo = ipinfo.map(line => {
      val fields = line.split("\\|")
      //结束IP
      val startIP = fields(2).toLong

      //开始IP
      val endIP = fields(3).toLong

      //获取省份
      val province = fields(6)

      (startIP, endIP, province)
    })

    //在广播变量之前,需要把要广播的数据获取到
    val arrIpInfo: Array[(Long, Long, String)] = splitedIPInfo.collect()

    // 对于经常用到的变量的值,为了避免在各个节点之间来回的读取增加网络IO负担,
    // 可以把该变量以广播的方式发送到参与计算的节点(executor),每个executor都能接收到该变量的值
    // 后期只用到改变了时,接可以直接从本地获取该变量的值
    // 这就是广播变量
    val broadcastIpInfo: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(arrIpInfo)

    // 获取用户访问数据
    val userAccessInfo = sc.textFile("")

    //切分数据并得到用户属于区域(省份)
    val provincTup: RDD[(String, Int)] = userAccessInfo.map(line => {
      val fields = line.split("\\|")
      val userIp = fields(1)
      //用户IP
      val ip2Long = ip2Long(userIp)
      //获取广播出去的ip基础数据
      val arrIp: Array[(Long, Long, String)] = broadcastIpInfo.value

      // 通过二分查找得到用户ip属于哪个ip段的索引
      val index = binarySearch(arrIp, ip2Long)

      // 查询用户属于哪个省份
      //val province = arrIp(index) _._3
      //(province, 1)
    })

    // 聚合得到用户所属区域的访问量
    val sumed: RDD[(String, Int)] = provincTup.reduceByKey(_ + _)

    sumed.foreachPartition(data2Mysql)

*/
  }


  val data2Mysql = (it: Iterator[(String, Int)]) => {
    var conn: Connection = null
    var ps: PreparedStatement = null
    var sql = "insert into location_info(location,counts,access_date) values(?,?,?)"
    val jdbc = "jdbc:mysql://192.168.93.111:3306/bigdata?useUnicode=true&characterEncoding=utf8"
    val user = "root"
    val passwd = "root"

    try {
      conn = DriverManager.getConnection(jdbc, user, passwd)
      it.foreach(x => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, x._1)
        ps.setInt(2, x._2)
        ps.setDate(3, new Date(System.currentTimeMillis()))
      })
    } catch {
      case e: Exception => println(e.printStackTrace())
    } finally {

      try {
        if (ps != null) ps.close()
      } catch {
        case e: Exception => e.printStackTrace()
      }


      try {
        if (conn != null) conn.close()
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }


  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }


  def binarySearch(arr: Array[(Long, Long, String)], ip: Long): Int = {
    var low = 0
    var hight = arr.length - 1
    while (low <= hight) {
      val mid = (low + hight) / 2
      if (ip >= arr(mid)._1 && ip <= arr(mid)._2) {
        return mid
      }
      if (ip < arr(mid)._1) {
        hight = mid - 1
      } else {
        low = mid + 1
      }

    }
    -1
  }
}
