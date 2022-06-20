package test;

import consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import producer.Producer;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

public class KafkaApplication {
    /**
     * 消息发布测试
     */
    public static void main(String[] args) {
        Producer<String, String> producer = new Producer<>();

        ProducerRecord<String, String> record = new ProducerRecord<>("topic-demo", "sync! Hello!");
        // 同步发送消息
        producer.syncSendMessage(record);

        ProducerRecord<String, String> record2 = new ProducerRecord<>("topic-demo", "async! Hello!");
        // 异步发送消息
        producer.asyncSendMessage(record2);
    }
}

/**
 * 消费者
 */
class ConsumerApplication {
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