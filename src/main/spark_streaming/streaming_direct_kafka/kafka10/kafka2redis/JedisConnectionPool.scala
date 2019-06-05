package streaming_direct_kafka.kafka10.kafka2redis

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * 功能描述:
 * 〈 Jedis 连接池 〉
 *
 * @param null
 * @return:
 * @since: 1.0.0
 * @Author:SiXiang
 * @Date: 2019/5/29 17:45
*/
object JedisConnectionPool {
  private val config = new JedisPoolConfig()
  //最大连接数
  config.setMaxTotal(20)
  //最大空闲连接数
  config.setMaxIdle(10)
  //当调用borrow Object方法时，是否进行有效性检查 -->
  config.setTestOnBorrow(true)
  //10000代表超时时间（10秒）
  private val pool = new JedisPool(config, "192.168.184.120", 6379, 10000, "123456")

  def getConnection(): Jedis = {
    pool.getResource
  }

}
