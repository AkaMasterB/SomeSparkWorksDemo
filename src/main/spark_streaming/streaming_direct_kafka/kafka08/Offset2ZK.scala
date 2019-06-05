//package streaming_direct_kafka.kafka08
//
//import kafka.common.TopicAndPartition
//import kafka.message.MessageAndMetadata
//import kafka.serializer.StringDecoder
//import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
//import org.I0Itec.zkclient.ZkClient
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
//import org.apache.spark.streaming.{Duration, StreamingContext}
//
///**
//  * 功能描述:
//  * 〈直连方式  将 offset 保存在 zk中〉
//  *
//  * @param null
//  * @return:
//  * @since: 1.0.0
//  * @Author:SiXiang
//  * @Date: 2018/5/28 10:20
//  */
//object Offset2ZK {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("ZKDirectKafka").setMaster("local[*]")
//    val ssc = new StreamingContext(conf, Duration(5000)) //ms
//
//    val groupId = "gp1"
//    val topic = "mytest"
//    //（SparkStreaming的程序，在消费数据的时候，直连到Kafka的分区上，而不去连接zk）
//    // 指定 kafka broker 地址（之前指定zk是为了消费数据是获取偏移量和元数据信息）
//    val brokerList = "192.168.184.120:9092,192.168.184.121:9092,192.168.184.122:9092"
//    // 指定 zk 用于保存 offset
//    val zkServer = "192.168.184.120:2181,192.168.184.121:2181,192.168.184.122:2181"
//    // 创建topic集合，用于消费多个topic
//    val topics = Set(topic)
//    //设置 kafka 参数
//    val kafkaParams = Map(
//      "metadata.broker.list" -> brokerList,
//      "group.id" -> groupId,
//      //从头消费
//      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
//    )
//
//    // 开始做 offset 保存前期工作（手动维护 offset）
//    val topicDirs = new ZKGroupTopicDirs(groupId, topic)
//    // 获取 zk 中的路径 /consumers/gp1/offsets/test
//    val zkTopicPath = topicDirs.consumerOffsetDir
//    // 获取 zkClient 可以获取偏移量数据，并且用于后面的更新
//    val zkClient = new ZkClient(zkServer)
//    //查询该路径下有没有对应目录,然后查出里面的分区
//    val partitions: Int = zkClient.countChildren(zkTopicPath)
//
//    // 如果zk保存offset，我们会用该offset做起始位置
//    // topicAndPartition  offset
//    var fromOffsets: Map[TopicAndPartition, Long] = Map()
//    // 创建Dstream
//    var kafkaDstream: InputDStream[(String, String)] = null
//
//    // 如果zk保存过offset
//    if (partitions > 0) {
//      for (i <- 0 until (partitions)) {
//        //        /consumers/gp1/offsets/test/0
//        //        /consumers/gp1/offsets/test/1
//        //        /consumers/gp1/offsets/test/2
//        // 获取 offset 值
//        val partitionOffset: String = zkClient.readData(s"${zkTopicPath}/${i}")
//        // 将不同 partition 对应的 offset 增加到fromOffset
//        val topicAndPartition = TopicAndPartition(topic, i)
//        fromOffsets += (topicAndPartition -> partitionOffset.toLong)
//      }
//      // key:kafka的Key，value：“123456789”
//      // 这个会将kafka的消息进行转换，最终kafka的数据都会变成Key，message这样的Tuple
//      val messageHandler = (mmd: MessageAndMetadata[String, String]) => {
//        (mmd.key(), mmd.message())
//      }
//      // 消费kafka数据
//      //  key     value    key的解码方式  value的解码方式
//      kafkaDstream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
//
//    } else {
//      // 如果未保存，根据kafka的配置从最开始的位置读取数据也就是offset
//      kafkaDstream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
//    }
//
//    // 定义全局变量offset范围，用于更新offset
//    var offsetRange = Array[OffsetRange]()
//
//    // 消费kafka数据
//    kafkaDstream
//      .transform(rdd => {
//        // 主要用于处理offset
//        //获取到offset 然后用于更新消费数据后的offset
//        offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//        rdd
//      })
//      .foreachRDD(rdd => {
//        // 注意：处理数据后要更新offset
//        rdd.map(_._2).foreach(x => println(x + "000000000000000"))
//        //更新 offset
//        for (o <- offsetRange) {
//          //取路径
//          val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
//          // 将partition中的 offset 保存到 zk
//          ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
//        }
//      })
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
