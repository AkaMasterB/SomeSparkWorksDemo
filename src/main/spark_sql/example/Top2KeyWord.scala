package example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ListBuffer

/**
 * 功能描述:
 * 1. 按照过滤条件进行过滤数据
 * 2. 将数据转换格式"日期_搜索词,用户名" 对它进行分组，对每天每个搜索词用户进行去重操作
 * 3. 并统计去重后的搜索词，用窗口函数实现
 * 4. 将格式转换为“日期_搜索词，UV”
*/
object Top2KeyWord {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Top2KeyWord")
      .master("local")
      .getOrCreate()


    //过滤条件
    val queryMap = Map(
      "city" -> List("beijing"),
      "platform" -> List("android"),
      "version" -> List("1.0","1.1","1.2","1.3")
    )
    //广播股过滤条件规则，防止外部变量资源浪费
    val queryBroadCast = spark.sparkContext.broadcast(queryMap)

    //读取数据
    val fileLogRDD = spark.sparkContext.textFile("E:\\千峰\\spark\\searchLog.txt")
    //切分数据，并案条件过滤
    val filterLogRDD = fileLogRDD.filter(t => {
      val map = queryBroadCast.value
      val cities = map.get("city").get
      val platforms = map.get("platform").get
      val versions = map.get("version").get

      val tmps = t.split(",")
      val city = tmps(3)
      val platform = tmps(4)
      val version = tmps(5)

      if (cities.contains(city) && platforms.contains(platform) && versions.contains(version)) {
        true
      } else {
        false
      }
    })
    //格式化
    val dataKeyWordRDD = filterLogRDD.map(t => {
      val tmps = t.split(",")
      ((tmps(0) + "_" + tmps(2)), (tmps(1)))
    })
    //分组，获取每天每个搜索词，有哪些用户搜索了
    val dataKeyWordUserRDD = dataKeyWordRDD.groupByKey()
    //去重
    val dateKeyWordUvRDD = dataKeyWordUserRDD.map(t => {
      //时间
      val date = t._1
      val usersIter = t._2.iterator

      val distinctUsers = new ListBuffer[String]
      while (usersIter.hasNext) {
        val usr = usersIter.next()
        if (!distinctUsers.contains(usr)) {
          distinctUsers.append(usr)
        }
      }

      val uv = distinctUsers.size
      (date, uv)
    })

    //整合数据
    val rowRDD = dateKeyWordUvRDD.map(row => {
      val tmps = row._1.split("_")
      Rows(tmps(0), tmps(1), row._2.toLong)
    })

    import spark.implicits._
//    val ds = spark.createDataset(rowRDD)
//    ds.createTempView("keyWord_uv")
//
//    spark.sql("select *,row_number() over(partition by date order by uv)rank from keyWord_uv")
//      .show()

//    ds.show()

    val df = rowRDD.toDF("date","keyword","uv")
    df.registerTempTable("keyword_uv")
    spark.sql("select *,row_number() over(partition by date order by uv)rank from keyWord_uv")
      .show()

  }

}
case class Rows(date:String,keyWord:String,uv:Long)