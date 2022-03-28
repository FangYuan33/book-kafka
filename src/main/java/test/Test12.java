package test;

import config.KafkaConfig;
import consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.time.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Test12 {

}
class Consumer12 {
    public static void main(String[] args) {
        // 在 Kafka 中每当消费者查找不到所记录的消费位移时，就会根据消费者客户端参数 auto.offset.reset
        // 配置来进行消费 latest从尾巴开始 earliest从头开始

        // 测试指定位置消费 seek
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(KafkaConfig.getConsumerProperties());
        // 订阅分区0
        TopicPartition topicPartition = new TopicPartition("topic-demo", 0);
        consumer.assign(Collections.singleton(topicPartition));

        // 拿出来所有的分区
        Set<TopicPartition> topicPartitions = consumer.assignment();
        // 指定从每个分区的10位置开始
        for (TopicPartition topicPartition2 : topicPartitions) {
            consumer.seek(topicPartition2, 10);
        }

        // 获取所有的分区尾部，并指定从尾部消费
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(Collections.singleton(topicPartition));
        consumer.seek(topicPartition, endOffsets.get(topicPartition));

        consumer.seekToEnd(Collections.singleton(topicPartition));

        // 从头开始消费
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(Collections.singleton(topicPartition));
        consumer.seek(topicPartition, beginningOffsets.get(topicPartition));

        consumer.seekToBeginning(Collections.singleton(topicPartition));

        // 指定时间消费 key为分区 value为时间戳
        Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
        timestampToSearch.put(topicPartition,
                LocalDateTime.of(LocalDate.now(), LocalTime.MIN).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
        // 为map中的所有分区指定时间戳
        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestampToSearch);

        consumer.seek(topicPartition, offsets.get(topicPartition).offset());

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
            }
        }
    }
}