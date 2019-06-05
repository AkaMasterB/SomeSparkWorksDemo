/**
 * FileName: KafkaConsumer
 * Author: SiXiang
 * Date: 2019/5/22 21:46
 * Description: kafka 消费者 consumer
 * History:
 * <author> <time> <version> <desc>
 * Sixiang 修改时间  版本号    描述
 */
package producer_consumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 功能简述：
 * 〈kafka 消费者 consumer〉
 *
 * @author SiXiang
 * @create 2019/5/22
 * @since 1.0.0
 */
public class KafkaConsumer extends Thread{
    private String topic;

    public KafkaConsumer(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public void run() {
        ConsumerConnector consumer = createConsumer();
        HashMap<String, Integer> topicCountMap = new HashMap<>();
        // 一次从主题中获取一个数据
        topicCountMap.put(topic,1);
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
        // 获取每次接收到的这个数据
        KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
        while (iterator.hasNext()){
            String message = new String(iterator.next().message());
            System.out.println("接收到：" + message);
        }

    }

    private ConsumerConnector createConsumer(){
        Properties props = new Properties();
        props.put("zookeeper.connect","192.168.184.120:2181,192.168.184.121:2181,192.168.184.122:2181");
        // 必须要使用别的组名称， 如果生产者和消费者都在同一组，则不能访问同一组内的topic数据
        props.put("group.id","group1");
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
    }

    public static void main(String[] args) {
        new KafkaConsumer("test").run();
    }
}
