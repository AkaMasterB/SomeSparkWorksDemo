//package transform
//
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
///**
//  * 实现RDD的转换操作
//  * 过滤广告黑名单
//  * 用户对我们的网站上的广告可以进行点击
//  * 点击之后，要进行实时计费，点一次，算一次钱
//  * 但是对于那些帮助某些无良商家刷广告的人，我们就设置一个黑名单系统
//  * 只要是黑名单中的用户点击的广告，就过滤
//  */
//object Transform {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//      .setAppName("updateState")
//      .setMaster("local[2]")
//    val ssc = new StreamingContext(conf, Seconds(5))
//
//    val arr = Array(("sx",true),("bsy",true))
//    val blackRDD = ssc.sparkContext.parallelize(arr)
//
//    val zk = "192.168.184.120:2181"
//    val groupId = "gp1"
//    val topics = Map[String,Int](
//      "test" -> 2
//    )
//    val inputDstream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zk,groupId,topics)
//
//    val users: DStream[(String, String)] = inputDstream.map(attr => (attr._2.split(" ")(1),attr._2))
//
//    val res = users.transform(user => {
//      val joinRDD = user.leftOuterJoin(blackRDD)
//      val value = joinRDD.filter(tuple => {
//        if (tuple._2._2.getOrElse(false)) {
//          false
//        } else {
//          true
//        }
//      })
//        .map(_._2._1)
//      value
//    })
//
//    res.print()
//    ssc.start()
//    ssc.awaitTermination()
//
//  }
//}
