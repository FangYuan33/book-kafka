package test;

import consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import producer.Producer;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

public class Test02 {
    public static void main(String[] args) {
        KafkaProducer<String, String> producer = Producer.getProducer();

        ProducerRecord<String, String> record = new ProducerRecord<>("topic-demo", "hello, Kafka");

        try {
            // send方法本身是异步的，不过调用get方法后可以使Future对象阻塞，直到响应成功
            RecordMetadata recordMetadata = producer.send(record).get();
            System.out.println(recordMetadata.toString());

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                } else {
                    System.out.println("callBack" + metadata.toString());
                }
            });

            producer.close();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
class ConsumerClient02 {
    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = Consumer.getConsumer("topic-demo");

        // 循环消费消息
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
            }
        }
    }
}