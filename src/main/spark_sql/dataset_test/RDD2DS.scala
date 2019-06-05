package dataset_test

import org.apache.spark.sql.SparkSession

object RDD2DS {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RDD2DS")
      .master("local")
      .getOrCreate()


    import spark.implicits._
    val peopleDF = spark.sparkContext.textFile("D:\\IdeaProjects\\spark_work\\src\\main\\resources\\teacher.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    peopleDF.createOrReplaceTempView("people")

    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

//    teenagersDF.map(teenager => "name:" + teenager(0)).show()
//    teenagersDF.map(teenager => "name:" + teenager.getAs[String]("name")).show()


    //No pre-defined encoders for Dataset[Map[K,V]], define explicitly
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String,Any]]

    // Primitive types and case classes can be also defined as
    // implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

    val maps: Array[Map[String, Any]] = teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name","age"))).collect()
    maps.foreach(println)



  }
}
case class Person(name:String,age:Int)