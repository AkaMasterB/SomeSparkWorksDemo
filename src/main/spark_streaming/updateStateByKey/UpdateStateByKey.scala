//package updateStateByKey
//
//import Utils.{DBConnectionPool, JdbcHelper}
//import org.apache.spark.{HashPartitioner, SparkConf}
//import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object UpdateStateByKey {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//      .setAppName("updateState")
//      .setMaster("local[*]")
//    val ssc = new StreamingContext(conf,Seconds(5))
//
//    val zk = "192.168.184.120:2181"
//    val groupId = "gp1"
//    val topics = Map[String,Int](
//      "test" -> 2
//    )
//    ssc.checkpoint("D:\\IdeaProjects\\spark_work\\checkpoint")
//
//    val inputStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zk,groupId,topics)
//
//    //    inputStream.foreachRDD(each => {
//    //      each.flatMap(_._2.split(" "))
//    //        .map((_,1))
//    //        .reduceByKey(_+_)
//    //    })
//
//    val reduced: DStream[(String, Int)] = inputStream
//      .flatMap(_._2.split(" "))
//      .map((_, 1))
//      .updateStateByKey(updateFunc,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)
////    reduced.print()
//
//    reduced.foreachRDD(attr =>{
//      attr.foreachPartition(insertData)
//    })
//
//    ssc.start()
//    ssc.awaitTermination()
//
//
//
//  }
//
//  // updateFunc : (Iterator[(K,Seq[V],Option[S])]) => Iterator[(K,S)]
//  // 第一个参数是从 kafka 获取到的元素，Key值，String 类型
//  // 第二个参数是进行单词计数统计的 value值，Int类型
//  // 第三个参数是每次批次提交的中间结果集
//
//  val updateFunc = (iter:Iterator[(String,Seq[Int],Option[Int])]) => {
//    iter.map(attr => {
//      (attr._1,attr._2.sum + attr._3.getOrElse(0))
//    })
//  }
//
//  def insertData(iterator: Iterator[(String,Int)]): Unit ={
//    val conn = DBConnectionPool.getConn()
//    iterator.foreach(t =>{
//      JdbcHelper.update("insert into tb_update_state (word,count) values (?,?)",t._1,t._2)
//    })
//    DBConnectionPool.releaseCon(conn)
//  }
//}
