package config;

import interceptor.PrefixProducerInterceptor;
import interceptor.PrefixProducerInterceptor02;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaConfig {
    private static final String BROKER_LIST = "101.43.163.223:9092";

    private static final String GROUP_ID = "group.demo";

    public static Properties getProducerProperties() {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        // 序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        // 指定拦截器，甚至可以指定好几个拦截器，越排在后边儿的越先执行，形成一个调用链
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                PrefixProducerInterceptor02.class.getName() + "," + PrefixProducerInterceptor.class.getName());
        // 指定发送消息缓冲区的大小 默认32MB
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        // send最长的阻塞时间 默认60s
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000");

        return properties;
    }

    public static Properties getConsumerProperties() {
        Properties properties = new Properties();

        // 反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        return properties;
    }
}
