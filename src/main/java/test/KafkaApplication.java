package test;

import consumer.Consumer;
import domain.Company;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
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
        // 这里指定了消费分区1的消息，生产者也对应向分区1发送
        Consumer<String, Company> consumer = new Consumer<>("topic-demo");

        List<PartitionInfo> partitionInfos = consumer.partitionsFor("topic-demo");
        for (PartitionInfo partitionInfo : partitionInfos) {
            log.info("Topic: {}-Partition: {}", partitionInfo.topic(), partitionInfo.partition());
        }

        // 下面这个例子验证 当前的消费位移 + 1 = 最新的提交位移 or 下一条消息消费的位移

        // 当前消费到的位移
        long lastConsumeOffset = -1;
        // 循环消费消息
        while (true) {
            ConsumerRecords<String, Company> records = consumer.poll(Duration.ofMillis(100));

            if (records.isEmpty()) {
                break;
            }

            for (ConsumerRecord<String, Company> record : records) {
                lastConsumeOffset = record.offset();
            }
            // 同步提交消费位移
            consumer.commitSync();
        }
        log.info("LastConsumeOffset: {}", lastConsumeOffset);
        // 获取指定分区的最新提交的消费位移
        OffsetAndMetadata metadata = consumer.committed(new TopicPartition("topic-demo", 1));
        log.info("CommittedOffset: {}", metadata.offset());
        // 获取下一条消息消费的位移
        long position = consumer.position(new TopicPartition("topic-demo", 1));
        log.info("NextMessagePosition: {}", position);
    }
}