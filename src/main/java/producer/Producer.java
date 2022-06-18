package producer;

import config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * 消息的发布者
 */
public class Producer {

    private static final KafkaProducer<String, String> producer;

    static {
        producer = new KafkaProducer<>(KafkaConfig.getProducerProperties());
    }

    public static KafkaProducer<String, String> getProducer() {
        return producer;
    }
}
