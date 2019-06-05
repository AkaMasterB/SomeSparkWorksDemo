/**
 * FileName: Producers
 * Author: SiXiang
 * Date: 2019/5/24 10:01
 * Description:
 * History:
 * <author> <time> <version> <desc>
 * Sixiang 修改时间  版本号    描述
 */
package producer_consumer_v2;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.util.Properties;

/**
 * 生产者
*/
public class Producers {
    //主题
    private static final String topic = "test";

    public static void main(String[] args) {
        Properties props = new Properties();
//        props.put("zookeeper.connect","192.168.184.120:2181,192.168.184.121:2181,192.168.184.122:2181");
        props.put("serializer.class", StringEncoder.class.getName());
        //声明 kafka broker
        props.put("metadata.broker.list","192.168.184.120:9092");

        Producer producer = new Producer<>(new ProducerConfig(props));

        int i = 0;
        while (true){
            producer.send(new KeyedMessage(topic,"msg:"+i));
            ++i;
        }

    }
}
