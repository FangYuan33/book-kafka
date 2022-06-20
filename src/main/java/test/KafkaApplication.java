package test;

import consumer.Consumer;
import domain.Company;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import producer.Producer;

import java.time.Duration;

public class KafkaApplication {
    /**
     * 消息发布测试
     */
    public static void main(String[] args) {
        Producer<String, Company> producer = new Producer<>();

        Company company = new Company();
        company.setName("JD");
        company.setAddress("BJ");
        ProducerRecord<String, Company> message = new ProducerRecord<>("topic-demo", company);

        producer.syncSendMessage(message);
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