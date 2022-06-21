package consumer;

import config.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;

/**
 * 消费者
 */
public class Consumer<K, V> {

    private final KafkaConsumer<K, V> consumer = new KafkaConsumer<>(KafkaConfig.getConsumerProperties());

    public Consumer(String topic) {
        consumer.subscribe(Collections.singletonList(topic));
    }

    public ConsumerRecords<K, V> poll(Duration duration) {
        return consumer.poll(duration);
    }
}
