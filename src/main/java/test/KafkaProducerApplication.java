package test;

import org.apache.kafka.clients.producer.ProducerRecord;
import producer.Producer;

public class KafkaProducerApplication {
    public static void main(String[] args) {
        Producer<String, String> producerString = new Producer<>();


        for (int i = 0; i < 2; i++) {
            // 测试Kafka消息发布
            ProducerRecord<String, String> record = new ProducerRecord<>("fang-yuan", "Hello, Kafka", "hello, Kafka!" + i);
            sendStringMessage(producerString, record);
        }
    }

    /**
     * 发送String类型的消息
     */
    private static void sendStringMessage(Producer<String, String> producerString, ProducerRecord<String, String> record) {
        producerString.syncSendMessage(record);
    }
}
