package test;

import consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;

public class Test10 {

}
class Consumer10 {
    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = Consumer.getConsumer("topic-demo");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (TopicPartition topicPartition : records.partitions()) {

                // 获取指定分区的消息
                for (ConsumerRecord<String, String> record : records.records(topicPartition)) {
                    System.out.println(record);
                }
            }

            // 获取指定话题的消息
            records.records("topic-demo");
        }
    }
}
