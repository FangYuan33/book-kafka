package test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import producer.Producer;

import java.util.concurrent.ExecutionException;

public class Test04 {
    public static void main(String[] args) {
        KafkaProducer<String, String> producer = Producer.getProducer();

        ProducerRecord<String, String> record = new ProducerRecord<>("topic-demo", "hello, Kafka");

        try {
            for (int i = 0; i < 10; i++) {
                producer.send(record).get();
            }
            producer.close();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
