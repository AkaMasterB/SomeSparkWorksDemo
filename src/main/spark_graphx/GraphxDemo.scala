import org.apache.spark.sql.SparkSession

object GraphxDemo {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .appName("GraphxDemo")
      .master("local")
      .getOrCreate()
    session.sparkContext.makeRDD(Seq(
      (1L,("伦纳德",23)),
      (2L,("库里",34)),
      (6L,("西亚卡姆",32)),
      (9L,("汤普森",15)),
      (133L,("格林",10)),
      (16L,("洛瑞",18)),
      (21L,("小加",20)),
      (44L,("卡卡",26)),
      (138L,("罗纳尔多",40)),
      (158L,("J罗",26)),
      (5L,("齐达内",50)),
      (7L,("梅西",31))
    ))
  }
}
