package streaming_direct_kafka.kafka10;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerTest extends Thread {
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;

    /*isAsync同步、异步*/
    public ProducerTest(String topic, Boolean isAsync) {
        Properties properties = new Properties();
        /*加载配置文件*/
        try {
            properties.load(ProducerTest.class.getClassLoader().getResourceAsStream("kafka_producer.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        producer = new KafkaProducer<>(properties);
        this.topic = topic;
        this.isAsync = isAsync;
    }

    public static void main(String[] args) {
        ProducerTest test = new ProducerTest("testLog", true);
        test.start();
    }

    public void run() {
        int messageNo = 1;
        try {
            BufferedReader bf = new BufferedReader(
                    new FileReader(
                            new File(
                                    "D:\\IdeaProjects\\spark_work\\src\\main\\resources\\word.txt")));
            String messageStr = null;
            while ((messageStr = bf.readLine()) != null) {

                long startTime = System.currentTimeMillis();

                if (isAsync) { // Send asynchronously
                    producer.send(new ProducerRecord<>(topic, messageNo, messageStr),
                            new DemoCallBack(startTime, messageNo, messageStr));
                } else { // Send synchronously
                    try {
                        producer.send(new ProducerRecord<>(topic,
                                messageNo,
                                messageStr)).get();
                        Thread.sleep(5000);
                        System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                }
                ++messageNo;
            }
            bf.close();
            producer.close();
            System.out.println("已经发送完毕");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
