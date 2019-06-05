package dataset_test

import org.apache.spark.sql.{Dataset, SparkSession}

object DataSet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("data_set")
      .master("local")
      .getOrCreate()
    val df = spark.read.json("E:\\千峰\\spark\\day11\\a.json")
    df.cache()
    df.createTempView("tb_a")

    import spark.implicits._
    val prod: Dataset[Pro] = df.as[Pro]

    prod.show()
  }
}

case class Pro(name:String,age:Long,depId:Long,gender:String,salary:Long)
