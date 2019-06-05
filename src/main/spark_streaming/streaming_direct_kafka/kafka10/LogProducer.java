package streaming_direct_kafka.kafka10;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class LogProducer extends Thread{
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;

    /*isAsync同步、异步*/
    public LogProducer(String topic, Boolean isAsync) {
        Properties properties = new Properties();
        /*加载配置文件*/
        try {
            properties.load(LogProducer.class.getClassLoader().getResourceAsStream("kafka_producer.properties"));
        } catch (IOException e) {

            e.printStackTrace();
        }
        producer = new KafkaProducer<>(properties);
        this.topic = topic;
        this.isAsync = isAsync;
    }

    public void run() {
        int messageNo = 1;
        long events = 100;
        Random rnd = new Random();
        java.util.Random points =new java.util.Random(2);

        for (long nEvents = 0; nEvents < events; nEvents++){

            long runtime = new Date().getTime();
            String ip = "192.168.2." + rnd.nextInt(255);
            String messageStr = runtime + ",www.example.com," + ip;

            long startTime = System.currentTimeMillis();
            if (isAsync) { // Send asynchronously
                producer.send(new ProducerRecord<>(topic,
                        messageNo,
                        messageStr), new DemoCallBack(startTime, messageNo, messageStr));
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else { // Send synchronously
                try {
                    producer.send(new ProducerRecord<>(topic,
                            messageNo,
                            messageStr)).get();
                    System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            ++messageNo;
        }
    }

    public static void main(String[] args) {
        LogProducer test = new LogProducer("testLog", true);
        test.start();
    }
}
