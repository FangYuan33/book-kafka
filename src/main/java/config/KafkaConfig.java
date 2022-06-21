package config;

import deserializer.CompanyDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import serializer.CompanySerializer;

import java.util.Properties;

/**
 * Kafka配置
 */
public class KafkaConfig {
    private static final String BROKER_LIST = "xxx:9092";

    private static final String GROUP_ID = "group.demo";

    public static Properties getProducerProperties() {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        // 序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());
        // 用来设定 KafkaProducer 对应的客户端id
        // 如果客户端不设置，则 KafkaProducer 会自动生成一个非空字符串，内容形式如“producer-1”、“producer-2”
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        // 指定发送消息缓冲区的大小 默认32MB
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        // send最长的阻塞时间 默认60s
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000");
        // 消息发送重试次数，有些异常可以通过重试来解决，比如NetworkException，但是像RecordTooLargeException异常就不能通过重试解决
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);

        return properties;
    }

    public static Properties getConsumerProperties() {
        Properties properties = new Properties();

        // 反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CompanyDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        return properties;
    }

}
