package application;

import config.KafkaConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 消费者的多线程实现
 *
 * 线程封闭：即一个线程内只有一个消费者
 *
 * @author FangYuan
 * @since 2023-03-03 20:56:57
 */
public class MultipleKafkaConsumerApplication {
    public static void main(String[] args) {
        // 开启3条线程消费该主题的3个分区，每个线程中又有一个3条线程大小的线程池来消费消息
        for (int i = 0; i < 3; i++) {
            new KafkaConsumerThread("fang-yuan", 3).start();
        }
    }
}

@Slf4j
class KafkaConsumerThread extends Thread {

    private final KafkaConsumer<String, String> kafkaConsumer;

    /**
     * 用于消费消息执行业务逻辑的线程池
     */
    private final ExecutorService executorService;

    public KafkaConsumerThread(String topic, int threadNumber) {
        kafkaConsumer = new KafkaConsumer<>(KafkaConfig.getConsumerProperties());
        kafkaConsumer.subscribe(Collections.singleton(topic));

        // 使用 CallerRunsPolicy 策略，在线程池中如果任务已满的话，在提交任务的线程中执行消费
        executorService = new ThreadPoolExecutor(threadNumber, threadNumber, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Override
    public void run() {

        try {
            while (true) {
                // 线程消费消息执行
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

                // 将拉取消息和处理消息解耦，处理消息用线程池去执行
                if (!records.isEmpty()) {
                    executorService.submit(new MessageHandler(records, kafkaConsumer));
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            kafkaConsumer.close();
        }
    }
}

@Slf4j
class MessageHandler implements Runnable {

    /**
     * 用来保存分区对应的消费位移，以防由于多线程消费消息发生消息消费位移覆盖的情况
     * 需要配置自动提交为 false
     */
    private static final HashMap<TopicPartition, Long> topicPartitionOffset = new HashMap<>();

    private final ConsumerRecords<String, String> records;

    private final KafkaConsumer<String, String> kafkaConsumer;

    public MessageHandler(ConsumerRecords<String, String> records, KafkaConsumer<String, String> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
        this.records = records;
    }

    @Override
    public void run() {
        // 处理消息
        Set<TopicPartition> partitions = records.partitions();

        for (TopicPartition partition : partitions) {
            // 按照分区进行处理
            List<ConsumerRecord<String, String>> records = this.records.records(partition);

            if (!records.isEmpty()) {
                // 消费消息
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Consume Record, Topic {} Partition {}", record.topic(), record.partition());
                }

                // 获取最后一条分区消息的位移
                long lastMessageOffset = records.get(records.size() - 1).offset();

                // 加锁防并发处理
                synchronized (topicPartitionOffset) {
                    if (topicPartitionOffset.containsKey(partition)) {
                        // 如果该分区已经提交过消费位移，则对当前消费的消费位移进行比较，大的话保存上
                        if (topicPartitionOffset.get(partition) < lastMessageOffset) {
                            topicPartitionOffset.put(partition, lastMessageOffset);

                            // 同步提交消费位移
                            kafkaConsumer.commitSync();
                        }
                    } else {
                        // 该分区没提交过的话，直接保存上
                        topicPartitionOffset.put(partition, lastMessageOffset);

                        // 同步提交消费位移
                        kafkaConsumer.commitSync();
                    }
                }
            }
        }
    }
}
