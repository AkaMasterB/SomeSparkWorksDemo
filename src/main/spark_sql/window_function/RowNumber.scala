package window_function

import org.apache.spark.sql.SparkSession

object RowNumber {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RowNumber")
      .master("local")
      .getOrCreate()
    val file = spark.sparkContext.textFile("E:\\千峰\\spark\\day11\\teacher.txt")
    val teacherRDD = file.map(t => {
      val fields = t.split(" ")
      Teacher(fields(0), fields(1))
    })

    import spark.implicits._
    val df = teacherRDD.toDF("subject","teacher")
    df.registerTempTable("teacher")
    val df2 = spark.sql("select subject,teacher,count(*) as counts from teacher group by subject,teacher")
    df2.registerTempTable("tmp")

//    val res = spark.sql("select *,row_number() over(partition by subject order by counts desc) as rank from tmp")
//    val res = spark.sql("select *,rank() over(partition by subject order by counts desc) as rank from tmp")
    val res = spark.sql("select *,dense_rank() over(partition by subject order by counts desc) as rank from tmp")
    res.show()



  }
}

case class Teacher(a:String,b:String)