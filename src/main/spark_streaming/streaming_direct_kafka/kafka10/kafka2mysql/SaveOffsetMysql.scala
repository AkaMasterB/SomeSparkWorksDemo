package streaming_direct_kafka.kafka10.kafka2mysql

import java.sql.Timestamp

import net.sf.json.{JSONArray, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 功能描述:
  * 〈 Save Offset to MySQL 〉
  *
  * @since: 1.0.0
  * @Author:SiXiang
  * @Date: 2019/5/30 21:43
  */

object SaveOffsetMysql {
  //过滤日志
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Offset2Redis").setMaster("local[*]")
    //每秒钟每个分区kafka拉取消息的速率
    //      .set("spark.streaming.kafka.maxRatePerPartition", "100")
    // 序列化
    //      .set("spark.serilizer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf, Seconds(5))
    // 启动一参数配置
    val topicsSet = Set("testLog")
    val groupId = "group_01"

    // kafka配置参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.184.120:9092,192.168.184.121:9092,192.168.184.122:9092",
      // kafka的key和value的解码方式
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      // 从最大开始消费
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val tpMap = getLastOffsets("sparkdb", "select offset from res where id = (select max(id) from res)")

    var stream: InputDStream[ConsumerRecord[String, String]] = null
    // 第一次消费数据，没有任何的消费信息数据
    if (!tpMap.nonEmpty) {
      stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        // 订阅 topic,[KafkaParams]
        ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
      )
    } else {
      // 如果mysql内有offset记录
      stream = KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        // [TopicPartition],[KafkaParams],[TopicPartition,Long]
        ConsumerStrategies.Assign[String, String](tpMap.keys, kafkaParams, tpMap)
      )
    }

    stream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val offset = offsetRanges2json(offsetRanges).toString()
      println("========= > " + offset)

      // 聚合操作的处理： 批处理 一个分区 一个事务
      rdd.map(_.value())
        .flatMap(_.split(" "))
        .map((_, 1L))
        .reduceByKey(_ + _)
        .repartition(1)
        .foreachPartition(partition => {

          Class.forName("com.mysql.jdbc.Driver");
          if (partition.nonEmpty) {
            val conn = MySqlUtil.getConnection("sparkdb")

            conn.setAutoCommit(false)
            val psts = conn.prepareStatement("INSERT INTO res (word,count,offset,time) VALUES (?,?,?,?)")
            partition.foreach { case (word: String, count: Long) =>
              psts.setString(1, word)
              psts.setLong(2, count)
              psts.setString(3, offset)
              psts.setTimestamp(4, new Timestamp(System.currentTimeMillis()))
              psts.addBatch()
            }
            psts.executeBatch()
            conn.commit()
            psts.close()
            conn.close()
          }
        })
    })

    // 启动流
    ssc.start()
    ssc.awaitTermination()


  }

  /**
    * [{"partition":2,"fromOffset":52,"untilOffset":55,"topic":"testLog"},
    * {"partition":0,"fromOffset":44,"untilOffset":47,"topic":"testLog"},
    * {"partition":1,"fromOffset":60,"untilOffset":63,"topic":"testLog"}]
    */
  // 编织成 json
  def offsetRanges2json(arr: Array[OffsetRange]): JSONArray = {
    val jSONArray = new JSONArray()
    arr.foreach(offsetRange => {
      val jsonObject = new JSONObject()
      jsonObject.accumulate("partition", offsetRange.partition)
      jsonObject.accumulate("fromOffset", offsetRange.fromOffset)
      jsonObject.accumulate("untilOffset", offsetRange.untilOffset)
      jsonObject.accumulate("topic", offsetRange.topic)
      jSONArray.add(jsonObject)
    })
    return jSONArray
  }

  // 获取最后一个offset记录
  def getLastOffsets(db: String, sql: String): Map[TopicPartition, Long] = {
    val conn = MySqlUtil.getConnection(db)
    val psts = conn.prepareStatement(sql)
    val res = psts.executeQuery
    var tpMap: Map[TopicPartition, Long] = Map[TopicPartition, Long]()
    while (res.next) {
      val o = res.getString(1)
      val jSONArray = JSONArray.fromObject(o)
      jSONArray.toArray().foreach(offset => {
        val json = JSONObject.fromObject(offset)
        val topicPartition = new TopicPartition(json.getString("topic"), json.getInt("partition"))
        tpMap += (topicPartition -> json.getLong("untilOffset"))
      })

    }
    psts.close()
    conn.close()
    return tpMap
  }

}
