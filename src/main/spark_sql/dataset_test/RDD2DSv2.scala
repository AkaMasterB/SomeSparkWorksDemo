package dataset_test

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object RDD2DSv2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RDD2DS")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val peopleRDD = spark.sparkContext.textFile("D:\\IdeaProjects\\spark_work\\src\\main\\resources\\teacher.txt")

    val schemaStr = "name age"

    val fields = schemaStr.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))

    val schema: StructType = StructType(fields)

    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    val peopleDF = spark.createDataFrame(rowRDD,schema)

    peopleDF.createOrReplaceTempView("people")

    val res = spark.sql("SELECT name FROM people")

    res.map(attributes => "name:"+attributes(0)).show()
  }
}
