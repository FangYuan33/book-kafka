package test;

import consumer.Consumer;
import domain.Company;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import producer.Producer;

import java.time.Duration;
import java.util.List;

public class KafkaApplication {
    /**
     * 消息发布测试
     */
    public static void main(String[] args) {
        Producer<String, Company> producer = new Producer<>();

        Company company = new Company();
        company.setName("JD");
        company.setAddress("BJ");
        // 指定分区发送
        ProducerRecord<String, Company> message = new ProducerRecord<>("topic-demo", 1, null, company);

        producer.syncSendMessage(message);
    }
}

/**
 * 消费者
 */
@Slf4j
class ConsumerApplication {
    public static void main(String[] args) {
        Consumer<String, Company> consumer = new Consumer<>("topic-demo");

        List<PartitionInfo> partitionInfos = consumer.partitionsFor("topic-demo");
        for (PartitionInfo partitionInfo : partitionInfos) {
            log.info("Topic: {}-Partition: {}", partitionInfo.topic(), partitionInfo.partition());
        }

        // 循环消费消息
        while (true) {
            ConsumerRecords<String, Company> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, Company> record : records) {
                log.info("Topic: {}, Partition: {}, Offset: {}, key: {}, value: {}",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }
        }

        // 取消订阅
//        consumer.unsubscribe();
    }
}