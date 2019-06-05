/**
 * FileName: KafkaProducer
 * Author: SiXiang
 * Date: 2019/5/22 21:01
 * Description: kafka生产者 producer
 * History:
 * <author> <time> <version> <desc>
 * Sixiang 修改时间  版本号    描述
 */
package producer_consumer;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 功能简述：
 * 〈kafka生产者 producer〉
 *
 * @author SiXiang
 * @create 2019/5/22
 * @since 1.0.0
 */
public class KafkaProducer extends Thread{
    private String topic;

    public KafkaProducer(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public void run() {
        Producer producer = createProducer();
        int i = 0;
        while (true){
            producer.send(new KeyedMessage<Integer,String>(topic,"message:"+i++));
            try {
                TimeUnit.SECONDS.sleep(1);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }

    private Producer createProducer(){
        Properties props = new Properties();
        //声明 zk
        props.put("zookeeper.connect","192.168.184.120:2181,192.168.184.121:2181,192.168.184.122:2181");
        props.put("serializer.class", StringEncoder.class.getName());
        //声明 kafka broker
        props.put("metadata.broker.list","192.168.184.120:9092,192.168.184.121:9092,192.168.184.122:9092");
        return new Producer<Integer,String>(new ProducerConfig(props));
    }

    public static void main(String[] args) {
        //使用kafka集群中创建好的topic
        new KafkaProducer("test").start();
    }
}
