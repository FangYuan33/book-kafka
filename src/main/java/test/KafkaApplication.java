package test;

import com.alibaba.fastjson.JSON;
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

        // 循环消费消息
        while (true) {
            ConsumerRecords<String, Company> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, Company> record : records) {
                log.info("---处理业务逻辑---, {}, partition: {}", record.value(), record.partition());
            }
            // 同步提交消费位移
            consumer.commitSync();
        }
    }
}