package test;

import config.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import producer.Producer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class Test11 {

}
class Cousumer11 {
    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(KafkaConfig.getConsumerProperties());

        // 订阅分区0
        TopicPartition topicPartition = new TopicPartition("topic-demo", 0);
        consumer.assign(Collections.singleton(topicPartition));

        long lastConsumedOffset;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            List<ConsumerRecord<String, String>> positionRecords = records.records(topicPartition);

            if (!positionRecords.isEmpty()) {
                // 暂停消费
                consumer.pause(Collections.singleton(topicPartition));
                // 重新开启消费
                consumer.resume(Collections.singleton(topicPartition));
                // 返回被暂停的所有话题分区集合
                Set<TopicPartition> paused = consumer.paused();

                // 获取当前消费到的位置
                lastConsumedOffset = positionRecords.get(positionRecords.size() - 1).offset();

                System.out.println("LastConsumedOffset: " + lastConsumedOffset);

                // 同步 提交最后一条消费消息的offset 在kafka中保存
                consumer.commitSync();

                // 异步提交
                consumer.commitAsync();

                // 此时的 committedOffset = lastConsumedOffset + 1
                OffsetAndMetadata committedOffset = consumer.committed(topicPartition);
                System.out.println("CommittedOffset: " + committedOffset);

                // position代表的是下次消费拉取的offset
                long position = consumer.position(topicPartition);
                System.out.println("Position: " + position);
            }
        }

        // enable.auto.commit 默认为true 自动提交
        // auto.commit.interval.ms 自动提交的频率，默认为5ms
    }
}

class Producer11 {
    public static void main(String[] args) {
        KafkaProducer<String, String> producer = Producer.getProducer();

        // 推送消息到topic-demo下的分区0
        ProducerRecord<String, String> record = new ProducerRecord<>("topic-demo",  0,
                "Hello", "test offset");

        producer.send(record);

        producer.close();
    }
}