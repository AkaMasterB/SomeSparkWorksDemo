package sql_demo

import org.apache.spark.sql.{Dataset, SparkSession}

object OrderAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("OrderAnalysis")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val stockDS = spark.sparkContext.textFile("D:\\IdeaProjects\\spark_work\\src\\main\\resources\\tbStock.txt")
      .map(_.split(","))
      .map(attr => Stock(attr(0), attr(1), attr(2)))
      .toDS()

    stockDS.show()
    stockDS.createOrReplaceTempView("tbStock")

    val stockDetailDS = spark.sparkContext.textFile("D:\\IdeaProjects\\spark_work\\src\\main\\resources\\tbStockDetail.txt")
      .map(_.split(","))
      .map(attr => StockDetail(attr(0), attr(1).trim.toInt, attr(2), attr(3).trim.toInt, attr(4).trim.toDouble, attr(5).trim.toDouble))
      .toDS()

    stockDetailDS.show()
    stockDetailDS.createOrReplaceTempView("tbStockDetail")

    val dateDS = spark.sparkContext.textFile("D:\\IdeaProjects\\spark_work\\src\\main\\resources\\tbDate.txt")
      .map(_.split(","))
      .map(attr => Date(attr(0), attr(1).trim.toInt, attr(2).trim.toInt, attr(3).trim.toInt, attr(4).trim.toInt, attr(5).trim.toInt, attr(6).trim.toInt, attr(7).trim.toInt, attr(8).trim.toInt, attr(9).trim.toInt))
      .toDS()

    dateDS.show()
    dateDS.createOrReplaceTempView("tbDate")

    /**
     *  select
      *  c.years as year,
      *  count(distinct(a.ordernumber)) as total_orders,
      *  sum(b.amount) as total_amount
      * from tbStock a
      * join tbStockDetail b on a.ordernumber = b.ordernumber
      * join tbDate c on a.dateid = c.dateid
      * group by c.years
      * order by c.years
      *
    */

    spark.sql("select c.years as year, " +
      "count(distinct(a.ordernumber)) as total_orders, " +
      "sum(b.amount) as total_amount " +
      "from tbStock a " +
      "join tbStockDetail b on a.ordernumber = b.ordernumber " +
      "join tbDate c on a.dateid = c.dateid " +
      "group by c.years " +
      "order by c.years")
      .show()




  }
}

case class Stock(
                    ordernumber:String,
                    locationid:String,
                    dateid:String
                  ) extends Serializable

case class StockDetail(
                          ordernumber:String,
                          rownum:Int,
                          itemid:String,
                          number:Int,
                          price:Double,
                          amount:Double
                        ) extends Serializable

case class Date(
                   dateid:String,
                   years:Int,
                   theyear:Int,
                   month:Int,
                   day:Int,
                   weekday:Int,
                   week:Int,
                   quarter:Int,
                   period:Int,
                   halfmonth:Int
                 ) extends Serializable
