package consumer;

import config.KafkaConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;

/**
 * 消费者
 */
public class Consumer {

    private static final KafkaConsumer<String, String> consumer;

    static {
        consumer = new KafkaConsumer<>(KafkaConfig.getConsumerProperties());
    }

    /**
     * 订阅特定主题的消费者
     */
    public static KafkaConsumer<String, String> getConsumer(String topic) {
        consumer.subscribe(Collections.singletonList(topic));

        return consumer;
    }
}
