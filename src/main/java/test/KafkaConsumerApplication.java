package test;

import com.alibaba.fastjson.JSON;
import config.KafkaConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

@Slf4j
public class KafkaConsumerApplication {
    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(KafkaConfig.getConsumerProperties());

        TopicPartition topicPartition = new TopicPartition("jd", 0);
        consumer.assign(Collections.singleton(topicPartition));

        // 循环消费消息
        while (true) {
            seekToBeginOrEnd(consumer, consumer.assignment());

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                log.info("---处理业务逻辑---, {}, partition: {}", record.value(), record.partition());
            }
        }
    }

    /**
     * 指定从分区头或分区尾进行消费
     */
    private static void seekToBeginOrEnd(KafkaConsumer<String, String> realConsumer, Set<TopicPartition> assignment) {
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
