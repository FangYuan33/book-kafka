package test;

import consumer.Consumer;
import domain.Company;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;

public class MultiConsumerApplication {

    private static final Integer THREAD_NUM = 5;

    public static void main(String[] args) {
        for (int i = 0; i < THREAD_NUM; i++) {
            new KafkaConsumerThread<String, Company>("topic-demo").start();
        }
    }
}

@Slf4j
class KafkaConsumerThread<K, V> extends Thread {

    private Consumer<K, V> consumer;

    public KafkaConsumerThread(String topic) {
        this.consumer = new Consumer<>(topic);
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<K, V> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<K, V> record : records) {
                    log.info("{} 消费 {}", Thread.currentThread().getName(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}