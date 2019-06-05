package streaming_direct_kafka.kafka10.kafka2redis

import org.apache.kafka.common.TopicPartition

/**
  * 功能描述:
  * 〈 处理 topic、partition、offset 数据 〉
  *
  * @param null
  * @return:
  * @since: 1.0.0
  * @Author:SiXiang
  * @Date: 2019/5/29 17:46
  */
object JedisOffset {
  def apply(groupId: String) = {

    // 创建Map形式的Topic、partition、Offset
    var fromdbOffset = Map[TopicPartition, Long]()
    //获取Jedis连接
    val jedis = JedisConnectionPool.getConnection()
    // 查询出Redis中的所有topic partition
    val topicPartitionOffset = jedis.hgetAll(groupId)
    // 导入隐式转换
    import scala.collection.JavaConversions._
    // 将Redis中的Topic下的partition中的offset转换成List
    val topicPartitionOffsetlist: List[(String, String)] = topicPartitionOffset.toList
    // 循环处理所有的数据
    for (record <- topicPartitionOffsetlist) {
      val split: Array[String] = record._1.split("[-]")
      fromdbOffset += (
        new TopicPartition(split(0), split(1).toInt) -> record._2.toLong)
    }
    fromdbOffset
  }
}
