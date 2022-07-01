package test;

import com.alibaba.fastjson.JSON;
import consumer.Consumer;
import domain.Company;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import producer.Producer;

import java.time.Duration;
import java.util.*;

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

        KafkaConsumer<String, Company> realConsumer = consumer.getConsumer();

        // 循环消费消息
        while (true) {
            // 先调用一次poll为了拉取分配到该主题下的分区，分区的分配是在poll方法里进行的
            realConsumer.poll(Duration.ofSeconds(1));

            // 订阅的所有分区
            Set<TopicPartition> assignment = realConsumer.assignment();
            log.info("订阅的所有分区 {}", JSON.toJSONString(assignment));

//            seekToBeginOrEnd(realConsumer, assignment);

            seekTimeStamp(realConsumer, assignment);

            // 这下再拉取消息的话，要根据seek后的位移拉取了
            ConsumerRecords<String, Company> records = realConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Company> record : records) {
                log.info("---处理业务逻辑---, {}, partition: {}", record.value(), record.partition());
            }
        }
    }

    /**
     * chapter_12
     *
     * 指定时间点获取消费位移的seek调用
     */
    private static void seekTimeStamp(KafkaConsumer<String, Company> realConsumer, Set<TopicPartition> assignment) {
        // 指定分区和对应的时间戳
        HashMap<TopicPartition, Long> map = new HashMap<>();
        for (TopicPartition topicPartition : assignment) {
            map.put(topicPartition, System.currentTimeMillis() - 864000000L);
        }

        // 根据时间戳获取到对应的消费位移
        Map<TopicPartition, OffsetAndTimestamp> timeOffsets = realConsumer.offsetsForTimes(map);

        // 指定具体时间的时间戳时的消费位移之后进行消费
        for (TopicPartition topicPartition : assignment) {
            OffsetAndTimestamp offsetAndTimestamp = timeOffsets.get(topicPartition);

            if (offsetAndTimestamp != null) {
                realConsumer.seek(topicPartition, offsetAndTimestamp.offset());
            }
        }
    }

    /**
     * chapter_12
     *
     * 指定从分区头或分区尾进行消费
     */
    private static void seekToBeginOrEnd(KafkaConsumer<String, Company> realConsumer, Set<TopicPartition> assignment) {
        // 获取的消息将写入的位置
        Map<TopicPartition, Long> endOffsets = realConsumer.endOffsets(assignment);
        // 获取消息开始的位置
        Map<TopicPartition, Long> beginningOffsets = realConsumer.beginningOffsets(assignment);

        // 指定从订阅的每个分区开始从0处开始消费消息
        for (TopicPartition topicPartition : assignment) {
            realConsumer.seek(topicPartition, beginningOffsets.get(topicPartition));
        }

        // 当然也可以直接指定从开始或从末尾开始消费
//            realConsumer.seekToBeginning(assignment);
//            realConsumer.seekToEnd(assignment);
    }
}