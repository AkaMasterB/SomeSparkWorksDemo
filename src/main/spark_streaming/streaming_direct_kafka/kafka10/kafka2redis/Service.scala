package streaming_direct_kafka.kafka10.kafka2redis

import org.apache.spark.rdd.RDD
import streaming_direct_kafka.MyUtils

object Service {
  //总的成交总额
  def totalVolume(rdd: RDD[Array[String]]): Unit = {
    rdd.filter(_.length >= 4)
      .map(attr => {
        attr(4).toDouble
      })
      .foreachPartition(f => {
        val jedis = JedisConnectionPool.getConnection()
        f.foreach(t => {
          jedis.incrByFloat("total_volume", t)
        })
        jedis.close()
      })
  }

  //每个商品分类的成交量
  def countsByType(rdd: RDD[Array[String]]): Unit = {
    rdd.filter(_.length >= 4)
      .map(attr => {
        (attr(2), 1)
      })
      .reduceByKey(_ + _)
      .foreachPartition(f => {
        val jedis = JedisConnectionPool.getConnection()
        f.foreach(t => {
          jedis.hset("counts_type", t._1, t._2.toString)
        })
        jedis.close()
      })
  }

  //每个商品分类的成交总额，并按照从高到低排序
  def sortedVolumeByType(rdd: RDD[Array[String]]): Unit = {
    rdd.filter(_.length >= 4)
      .map(attr => {
        (attr(2), attr(4).toDouble)
      })
      .reduceByKey(_ + _)
      .foreachPartition(f => {
        val jedis = JedisConnectionPool.getConnection()
        f.foreach(t => {
          jedis.zadd("sorted_volume_type",t._2,t._1)
        })
        jedis.close()
      })
  }

  // 每个省份的成交总额
  def volumeByProvince(rdd: RDD[Array[String]], rules: Array[(Long, Long, String)]): Unit = {

    rdd.filter(_.length >= 4)
      .map(attr => {
        val ipNum = MyUtils.ip2Long(attr(1))
        val index: Int = MyUtils.binarySearch(rules, ipNum)
        var province: String =
          if (index != -1) {
            rules(index)._3
          } else {
            "未识别的ip"
          }
        (province, 1)
      })
      .reduceByKey(_ + _)
      .foreachPartition(f => {
        val jedis = JedisConnectionPool.getConnection()
        f.foreach(t => {
          jedis.hset("sorted_volume_type", t._1, t._2.toString)
        })
        jedis.close()
      })
  }

}
