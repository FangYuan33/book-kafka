package test;

import com.alibaba.fastjson.JSON;
import config.KafkaConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class KafkaConsumerApplication {
    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(KafkaConfig.getConsumerProperties());

        partitionsFor(consumer, "fang-yuan");

//        testConsumeMessage(consumer, "fang-yuan");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                log.info("---处理业务逻辑---, {}, partition: {}", record.value(), record.partition());
            }

            log.info("---指定分区的消息 {} ---", records(records, new TopicPartition("jd", 1)));
        }
    }

    /**
     * 测试消息的消费
     */
    private static <V> void testConsumeMessage(KafkaConsumer<String, V> consumer, String topic) {
        consumer.subscribe(Collections.singleton(topic));

        // 循环消费消息
        while (true) {
//            seekToBeginOrEnd(consumer, consumer.assignment());

            ConsumerRecords<String, V> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, V> record : records) {
                log.info("---处理业务逻辑---, {}, partition: {}", record.value(), record.partition());
            }
        }
    }

    /**
     * 获取某主题的分区信息
     */
    private static <V> List<PartitionInfo> partitionsFor(KafkaConsumer<String, V> consumer, String topic) {
        List<PartitionInfo> jdTopicPartitionInfo = consumer.partitionsFor(topic);
        log.info("主题 {} 的分区信息为 {}", "jd", jdTopicPartitionInfo);

        return jdTopicPartitionInfo;
    }

    /**
     * assign 订阅主题的分区进行消费
     */
    private static <V> void assignTopicPartition(KafkaConsumer<String, V> consumer, TopicPartition topicPartition) {
        consumer.assign(Collections.singleton(topicPartition));
    }

    /**
     * subscribe 订阅主题，具有消费者自动再均衡的动能，也就是说 多个消费者情况下可以根据分区分配策略自动分配各个消费者与分区的关系
     * 当消费组内的消费者增加或减少时，分区分配关系会自动调整，保证消费的负载均衡和故障自动转移
     */
    private static <V> void subscribeTopic(KafkaConsumer<String, V> consumer, String topic) {
        consumer.subscribe(Collections.singletonList(topic));
    }

    /**
     * 从已经拉取到的消息中，拿出指定分区的消息
     */
    private static <V> List<ConsumerRecord<String, V>> records(ConsumerRecords<String, V> records, TopicPartition partition) {
        return records.records(partition);
    }

    /**
     * 指定从分区头或分区尾进行消费
     */
    private static <V> void seekToBeginOrEnd(KafkaConsumer<String, V> realConsumer, Set<TopicPartition> assignment) {
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
