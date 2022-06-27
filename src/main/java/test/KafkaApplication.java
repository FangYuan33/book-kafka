package test;

import consumer.Consumer;
import domain.Company;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import producer.Producer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;

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

            // 获取消息的所有分区
            Set<TopicPartition> partitions = records.partitions();

            // 根据分区获取该分区下所有的消息
            for (TopicPartition partition : partitions) {
                List<ConsumerRecord<String, Company>> recordList = records.records(partition);

                for (ConsumerRecord<String, Company> record : recordList) {
                    log.info("Partition: {}, Value: {}", partition.partition(), record.value());
                }
            }

            // 根据话题来消费消息
            List<String> topicList = Collections.singletonList("topic-demo");

            for (String topic : topicList) {
                Iterable<ConsumerRecord<String, Company>> iterable = records.records(topic);

                for (ConsumerRecord<String, Company> record : iterable) {
                    log.info("Topic: {}, Value: {}", topic, record.value());
                }
            }

        }

        // 取消订阅
//        consumer.unsubscribe();
    }
}