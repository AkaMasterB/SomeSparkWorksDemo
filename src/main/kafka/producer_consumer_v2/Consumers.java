/**
 * FileName: Consumers
 * Author: SiXiang
 * Date: 2019/5/24 10:07
 * Description:
 * History:
 * <author> <time> <version> <desc>
 * Sixiang 修改时间  版本号    描述
 */
package producer_consumer_v2;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 消费者
 */
public class Consumers {
    //主题
    private static final String topic = "test";
    //生产者线程数
    private static final Integer threads = 2;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("zookeeper.connect","192.168.184.121:2181");

        //消费者组
        props.put("group.id","www");

        //从头消费
//        props.put("auto.offset.reset","smallest");
        
        //创建消费者
        ConsumerConnector consumerConn = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        
        //创建Map，用来存储多个topic信息，用于消费多个topic
        HashMap<String, Integer> topicMap = new HashMap<>();
        topicMap.put(topic,threads);
        
        //获取数据信息流
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumerConn.createMessageStreams(topicMap);

        //循环取值
        List<KafkaStream<byte[], byte[]>> kafkaStreams = messageStreams.get(topic);
        for (KafkaStream<byte[], byte[]> stream : kafkaStreams) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (MessageAndMetadata<byte[], byte[]> s : stream) {
                        String msg = new String(s.message());
                        System.out.println(msg);
                    }
                }
            }).start();
        }
    }
}
