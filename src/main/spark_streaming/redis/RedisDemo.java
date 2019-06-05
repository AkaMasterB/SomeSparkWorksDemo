/**
 * FileName: RedisDemo
 * Author: SiXiang
 * Date: 2019/5/28 15:58
 * Description:
 * History:
 * <author> <time> <version> <desc>
 * Sixiang 修改时间  版本号    描述
 */
package redis;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RedisDemo {
//    public static void main(String[] args) {
        // 创建Jedis
//        Jedis jedis = new Jedis("192.168.184.120", 6379);
//        jedis.auth("123456");
//        System.out.println(jedis.ping());

//    }

    //连接池操作
    JedisPool jedisPool = null;
    Jedis jedis = null;

    @Before
    public void connection(){
        jedisPool = new JedisPool("192.168.184.120", 6379);
        jedis = jedisPool.getResource();
        jedis.auth("123456");
        jedis.flushDB();
//        System.out.println(jedis.ping());
    }

    // String 添加单条数据
    @Test
    public void StrTest(){
        jedis.set("name0","sx");
        System.out.println(jedis.get("name0"));
    }

    // String 添加多条数据 json {"name":"value"}
    @Test
    public void StrsTest(){
        jedis.mset("name1","bsy","gender","女");
        System.out.println(jedis.mget("name1","gender"));
    }

    //  hash 添加单条数据
    @Test
    public void hashTest(){
        jedis.hset("h01","name02","关羽");
        System.out.println(jedis.hget("h01","name02"));
    }

    //  hash 添加多条数据
    @Test
    public void hashsTest(){
        Map map = new HashMap<>();
        map.put("name3","刘备");
        map.put("name4","张飞");
        jedis.hmset("h02",map);
        System.out.println(jedis.hmget("h02","name3","name4"));
    }

    //  list
    @Test
    public void listTest(){
        // 从头部添加，返回长度
        jedis.lpush("list01", "赵云","周瑜","诸葛亮");
        jedis.lpush("list01", "张飞");
        jedis.lpush("list01", "关羽");
        // 从尾部添加，返回长度
        jedis.rpush("list01", "刘备");
//        // 再次运行结果是:
//        jedis.lpush("list01", "赵云");
//        jedis.lpush("list02", "张飞");
//        jedis.lpush("list03", "关羽");
//        jedis.lpush("list04", "刘备");
////        //  从新添加一个集合
//        jedis.lpush("list05", "凯");
//        jedis.lpush("list06", "虞姬");
//        jedis.lpush("list07", "孙悟空");
        // 获取长度
        Long llen = jedis.llen("");
        // 删除指定素并返回
        Long lrem = jedis.lrem("", 1, "");
        // 删除第一个元素并返回
        String lpop = jedis.lpop("");
        // 修改元素
        String lset = jedis.lset("", 2, "");
        // 截取元素
        String ltrim = jedis.ltrim("", 0, -1);
        // 获取指定元素（通过下标索引）
        String users = jedis.lindex("users", 0);

        System.out.println(jedis.lrange("list01", 0, 20));
    }

    // set
    @Test
    public void setTest(){
        jedis.sadd("set01","我","你");
        jedis.sadd("set01","是");
        jedis.sadd("set01","司");
        jedis.sadd("set01","翔");
        System.out.println(jedis.smembers("set01"));

        jedis.sadd("set02","go","to","bathroom");
        // 差集
        Set<String> set1 = jedis.sdiff("set01", "set02");
        System.out.println(set1);
        // 交集
        Set<String> set2 = jedis.sinter("set01", "set02");
        System.out.println(set2);
        // 从集合汇总随机移除一个元素，返回被移除的元素
        Set<String> moved = jedis.spop("set01", 2);

    }

    // Zset
    // sorted Set 对数据进行排序
    // 根据score排序，score值越大，排名越靠后
    @Test
    public void sortedSetTest(){
        HashMap<String, Double> map = new HashMap<>();
        map.put("100", new Double(1));
        map.put("98", new Double(3));
        map.put("99", new Double(2));
        jedis.zadd("sorted01",map);
        // 0,1 取得集合中的元素 ，主要排序是根据 Double 数值进行排序。
        System.out.println(jedis.zrange("sorted01", 0, 10));

        Long zcount = jedis.zcount("", 1, 8);

        //获取指定范围的最后几个元素，先翻转在取值
        jedis.zrevrange("",0,4);

    }


}
