//package kafka_streaming_wc
//
//import kafka.serializer.StringDecoder
//import org.apache.kafka.clients.consumer.ConsumerConfig
//import org.apache.spark.SparkConf
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.dstream.ReceiverInputDStream
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
///**
// * Receiver方式
//*/
//object RecKafkaStreaming {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("KafkaStreaming").setMaster("local[*]")
//    val ssc = new StreamingContext(conf,Seconds(3))
//
//    val zookeeper = "192.168.184.120:2181,192.168.184.121:2181,192.168.184.122:2181"
//    val topic = "test"
//    val consumerGroup = "spark"
//
//    //将 kafka 参数映射为 map
//    val kafkaParam: Map[String, String] = Map[String, String](
//      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG	-> "org.apache.kafka.common.serialization.StringDeserializer",
//      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
//      ConsumerConfig.GROUP_ID_CONFIG -> consumerGroup,
//      "zookeeper.connect" -> zookeeper
//    )
//
//    //通过 KafkaUtil 创建 kafkaDSteam
//    val kafkaDSteam: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
//      ssc,
//      kafkaParam,
//      Map(topic -> 3),
//      StorageLevel.MEMORY_ONLY
//    )
//
//    kafkaDSteam.foreachRDD(rdd => {
//      rdd.flatMap(_._2.split(" "))  // (偏移量,值)  _._1 为偏移量，不需要
//        .map((_,1))
//        .reduceByKey(_+_)
//        .collect()
//        .foreach(println)
//    })
//
//    ssc.start()
//    ssc.awaitTermination()
//
//
//
//
//
//  }
//}
