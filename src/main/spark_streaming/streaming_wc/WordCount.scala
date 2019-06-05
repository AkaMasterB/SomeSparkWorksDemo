package streaming_wc

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("streaming_wc").setMaster("local[2]")
    //conf,时间
    val ssc = new StreamingContext(conf,Seconds(5))
    //首先，获取输入DStream，代表一个从数据源来持续不断地实时数据流（kafka，socket）
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.184.120",9999)
    //每一次操作就会创建新的DStream，其实就是底层RDD实现
    //实时数据处理也是一行行处理

    //处理数据
    val res: DStream[(String, Int)] = lines
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    res.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
