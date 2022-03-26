package producer;

import config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;


public class Producer {

    private static final KafkaProducer<String, String> producer;

    static {
        producer = new KafkaProducer<>(KafkaConfig.getProducerProperties());
    }

    public static KafkaProducer<String, String> getProducer() {
        return producer;
    }
}
