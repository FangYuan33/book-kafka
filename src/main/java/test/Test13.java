package test;

import config.KafkaConfig;
import interceptor.TTLConsumerInterceptor;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import producer.Producer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Test13 {

}

class Consumer13 {
    public static void main(String[] args) {
        Properties consumerProperties = KafkaConfig.getConsumerProperties();
        // 配置拦截器
        consumerProperties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, TTLConsumerInterceptor.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
        // 再均衡监听器 指分区的所属权从一个消费者转移到另一消费者
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        consumer.subscribe(Collections.singleton("topic-demo"), new ConsumerRebalanceListener() {
            // 再均衡开始之前和消费者停止读取消息之后被调用
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // 均衡开始之前把消费位移信息都提交上去，这样能让之后的消费者避免重复消费
                consumer.commitSync(currentOffsets);
                currentOffsets.clear();
            }

            // 重新分配分区之后和消费者开始读取消费之前被调用
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

            }
        });

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());

                // 保存对应主题分区的偏移量，待再均衡时，提交到kafka中保存
                currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1));
            }
        }
    }
}

class Producer13 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaProducer<String, String> producer = Producer.getProducer();

        String topic = "topic-demo";
        long ttlTime = 10 * 1000;

        // 这个超时收不到
        ProducerRecord<String, String> record1 = new ProducerRecord<>(topic, 0,
                System.currentTimeMillis() - ttlTime, null, "first-expire-data");
        producer.send(record1).get();

        // 这条能收到
        ProducerRecord<String, String> record2 = new ProducerRecord<>(topic, 0,
                System.currentTimeMillis(), null, "normal-data");
        producer.send(record2).get();

        // 这条更能收到
        ProducerRecord<String, String> record3 = new ProducerRecord<>(topic, 0,
                System.currentTimeMillis() + ttlTime, null, "last-expire-data");
        producer.send(record3).get();
    }
}
