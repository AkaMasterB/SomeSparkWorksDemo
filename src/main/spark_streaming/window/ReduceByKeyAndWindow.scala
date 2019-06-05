//package window
//
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
//import org.apache.spark.streaming.kafka.KafkaUtils
//
///**
// * 每隔 5秒，统计最近20秒内的单词出现频次
//  * 热点搜索词
//*/
//object ReduceByKeyAndWindow {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//      .setAppName("updateState")
//      .setMaster("local[2]")
//    val ssc = new StreamingContext(conf, Seconds(5))
//
//    val lineDstream = ssc.socketTextStream("192.168.184.120",9999)
//    val wordRDD = lineDstream
//      .flatMap(_.split(" "))
//      .map((_,1))
//    val reduceWindowDstream: DStream[(String, Int)] = wordRDD.reduceByKeyAndWindow((x:Int, y:Int) => x+y,Seconds(20),Seconds(5))
//    val res: DStream[(Int, String)] = reduceWindowDstream.transform(rdd => {
//      val tuples = rdd.map(t => t.swap)
//        .sortByKey(false)
//        .take(3)
//      ssc.sparkContext.makeRDD(tuples)
//    })
//
//    res.print()
//    ssc.start()
//    ssc.awaitTermination()
//
//
//  }
//}
