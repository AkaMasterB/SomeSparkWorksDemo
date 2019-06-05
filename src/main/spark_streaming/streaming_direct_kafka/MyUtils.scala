package streaming_direct_kafka

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

object MyUtils extends Serializable {
  // 将IP转为十进制
  def ip2Long(ip:String): Long ={
    val s = ip.split("[.]")
    var ipNum =0L
    for(i<-0 until s.length){
      ipNum = s(i).toLong | ipNum << 8L
    }
    ipNum
  }
  /**
   * 二分查找 ip
   * @param lines
   * @param ip
   * @return:int
   * @since: 1.0.0
   * @Author:SiXiang
   * @Date: 2019/5/16 14:37
  */
  def binarySearch(lines: Array[(Long,Long,String)],ip: Long): Int ={
    var low = 0
    var high = lines.length-1
    while (low <= high){
      var mid = (high + low) / 2
      if (ip >= lines(mid)._1 && ip <= lines(mid)._2){
        return mid
      }else if(ip < lines(mid)._1){
        high = mid - 1
      }else{
        low = mid + 1
      }
    }
    -1
  }

  def loadRules(path: String,sc: SparkContext): Broadcast[Array[(Long, Long, String)]]={
    val lines = sc.textFile(path)
    val rules = lines.map(t => {
      val fileds = t.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      (startNum,endNum,province)
    }).collect()
    sc.broadcast(rules)
  }

}
