//package mapWithState
//
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.{MapWithStateDStream, ReceiverInputDStream}
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
//
//object MapWithState {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//      .setAppName("updateState")
//      .setMaster("local[2]")
//    val ssc = new StreamingContext(conf, Seconds(5))
//    ssc.checkpoint("D:\\IdeaProjects\\spark_work\\checkpoint")
//    val zk = "192.168.184.120:2181"
//    val groupId = "gp1"
//    val topics = Map[String, Int](
//      "test" -> 2
//    )
//
//    val inputStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zk,groupId,topics)
//
//    val mappingFunc = (word:String, one:Option[Int], state:State[Int]) =>{
//      val sum = one.getOrElse(0) + state.getOption().getOrElse(0)
//      val output = (word,sum)
//      state.update(sum)
//      output
//    }
//
//    val res: MapWithStateDStream[String, Int, Int, (String, Int)] = inputStream
//      .flatMap(_._2.split(" "))
//      .map((_, 1))
////      .reduceByKey(_ + _)
//      .mapWithState(StateSpec.function(mappingFunc))
//
//    res.print()
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
