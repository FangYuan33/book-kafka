package test;

import config.KafkaConfig;
import consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import producer.Producer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class Test14 {
    public static void main(String[] args) {
        String topic = "topic-demo";
        KafkaConsumer<String, String> consumer = Consumer.getConsumer(topic);
        // 获取到该主题下所有的分区
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);

        for (PartitionInfo partitionInfo : partitionInfos) {
            new KafkaConsumerThread(partitionInfo).start();
        }

        while (true) {

        }
    }
}

// 每个线程里都创建一个consumer 一条线程对应一个分区进行消费
// 缺点也很明显，每个消费线程都要维护一个独立的TCP连接，如果分区数和 consumerThreadNum 的值都很大，那么会造成不小的系统开销。
class KafkaConsumerThread extends Thread {
    private final KafkaConsumer<String, String> kafkaConsumer;

    public KafkaConsumerThread(PartitionInfo partitionInfo) {
        kafkaConsumer = new KafkaConsumer<>(KafkaConfig.getConsumerProperties());
        TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());

        kafkaConsumer.assign(Collections.singleton(topicPartition));
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(Thread.currentThread().getName() + record.value());
                }
            }
        } finally {
            kafkaConsumer.close();
        }
    }
}

class TestKafkaConsumerThread02 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String topic = "topic-demo";
        KafkaConsumer<String, String> consumer = Consumer.getConsumer(topic);
        // 获取到该主题下所有的分区
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);

        new KafkaConsumerThread02(topic, partitionInfos.size()).start();

        // 不停的发消息 不停的消费
        KafkaProducer<String, String> producer = Producer.getProducer();
        int positionOffset = 0;
        while (true) {
            positionOffset++;
            ProducerRecord<String, String> record = new ProducerRecord<>("topic-demo",  positionOffset % 3,
                    "Hello", "test offset");
            producer.send(record).get();
        }
    }
}

// 创建了一个消费者线程，该线程中有一个线程池，线程池大小为分区大小，也就是说线程池中的线程1v1管控着消息的消费
// 该线程中也维护了一个消费者，负责拉取消息，拉到后，提交到线程池中进行消费
class KafkaConsumerThread02 extends Thread {
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final ExecutorService executorService;

    // 共享变量，保存不同分区的偏移量，使用加锁的方式进行安全的保存和提交清空操作
    // 但是仍然存在消息丢失的风险：线程2执行 100-200 的消息，线程1执行 0-100 的消息
    // 线程2提交后，但是线程1发生了异常，那么线程1中的消息就丢失了
    private static final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    public KafkaConsumerThread02(String topic, Integer positionNum) {
        kafkaConsumer = new KafkaConsumer<>(KafkaConfig.getConsumerProperties());
        kafkaConsumer.subscribe(Collections.singleton(topic));
        executorService = new ThreadPoolExecutor(positionNum, positionNum, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(10), new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Override
    public void run() {
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

            if (!records.isEmpty()) {
                executorService.submit(new RecordsHandler(records));
            }

            synchronized (offsets) {
                kafkaConsumer.commitSync(offsets);
                offsets.clear();
            }
        }
    }

    private static class RecordsHandler extends Thread {
        public final ConsumerRecords<String, String> records;

        public RecordsHandler(ConsumerRecords<String, String> records) {
            this.records = records;
        }

        @Override
        public void run() {
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> records = this.records.records(partition);

                // 业务处理
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(Thread.currentThread().getName() + record.value());
                }

                // 保存最后消费偏移量
                long lastConsumedOffset = records.get(records.size() - 1).offset() + 1;
                synchronized (offsets) {
                    if (!offsets.containsKey(partition)) {
                        offsets.put(partition, new OffsetAndMetadata(lastConsumedOffset));
                    } else {
                        long position = offsets.get(partition).offset();

                        if (lastConsumedOffset > position) {
                            offsets.put(partition, new OffsetAndMetadata(lastConsumedOffset));
                        }
                    }
                }
            }
        }
    }
}

