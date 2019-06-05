package Utils

import java.sql.{Connection, DriverManager}
import java.util.{LinkedList, Locale, ResourceBundle}
/**
  * 功能描述:
  * 〈 MySQL 数据库连接池 〉
  *
  * @param null
  * @return:
  * @since: 1.0.0
  * @Author:SiXiang
  * @Date: 2019/5/27 19:38
  */
object DBConnectionPool{

//  private val reader = ResourceBundle.getBundle("DBconf.properties",Locale.CHINA)
//  private val max_connection = reader.getString("max_connection") //连接池总数
//  private val connection_num = reader.getString("connection_num") //产生连接数
//  private var current_num = 0 //当前连接池已产生的连接数
//  private val pools = new LinkedList[Connection]() //连接池
//  private val driver = reader.getString("driver")
//  private val url = reader.getString("url")
//  private val username = reader.getString("username")
//  private val password = reader.getString("password")
  private val max_connection = "10"
  private val connection_num = "5"
  private var current_num = 0 //当前连接池已产生的连接数
  private val pools = new LinkedList[Connection]() //连接池
  private val driver = "com.mysql.jdbc.Driver"
  private val url = "jdbc:mysql://localhost:3306/sparkdb"
  private val username = "root"
  private val password = "123456"

  /**
    * 加载驱动
    */
  private def before() {
    if (current_num > max_connection.toInt && pools.isEmpty()) {
      println("busyness")
      Thread.sleep(2000)
      before()
    } else {
      Class.forName(driver)
    }
  }

  /**
    * 获得连接
    */
  private def initConn(): Connection = {
    val conn = DriverManager.getConnection(url, username, password)
    conn
  }

  /**
    * 初始化连接池
    */
  private def initConnectionPool(): LinkedList[Connection] = {
    AnyRef.synchronized({
      if (pools.isEmpty()) {
        before()
        for (i <- 1 to connection_num.toInt) {
          pools.push(initConn())
          current_num += 1
        }
      }
      pools
    })
  }

  /**
    * 获得连接
    */
  def getConn(): Connection = {
    initConnectionPool()
    pools.poll()
  }

  /**
    * 释放连接
    */
  def releaseCon(con: Connection) {
    pools.push(con)
  }


}
