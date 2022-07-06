package test;

import consumer.Consumer;
import domain.Company;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MultiConsumerApplication {

    public static void main(String[] args) {

        new KafkaConsumerThread<String, Company>("topic-demo").start();
    }
}

class KafkaConsumerThread<K, V> extends Thread {

    private Consumer<K, V> consumer;

    private final Integer THREAD_NUM = 5;

    /**
     * 单线程消费者拉取消息，线程池对消息进行消费
     */
    private ExecutorService executorService;

    public KafkaConsumerThread(String topic) {
        this.consumer = new Consumer<>(topic);

        executorService = new ThreadPoolExecutor(THREAD_NUM, THREAD_NUM, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<K, V> records = consumer.poll(Duration.ofSeconds(1));

                executorService.submit(new RecordHandler<>(records));
            }
        } finally {
            consumer.close();
        }
    }
}

@Slf4j
class RecordHandler<K, V> extends Thread {

    /**
     * 消费完消息对消费位移提交前将消费位移保存，可以用它来控制消费位移的提交
     *
     * 但是，如果 0-99的消息正在消费，100-199的消费已经完成并且提交位移，那么如果0-99消费出现问题，那么这就会造成消息丢失
     */
    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    private final ConsumerRecords<K, V> records;

    public RecordHandler(ConsumerRecords<K, V> records) {
        this.records = records;
    }

    @Override
    public void run() {

        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<K, V>> records = this.records.records(partition);

            for (ConsumerRecord<K, V> record : records) {
                log.info("{} 消费 {}", Thread.currentThread().getName(), record.value());
            }

            long lastOffset = records.get(records.size() - 1).offset();

            synchronized (offsets) {
                if (offsets.containsKey(partition)) {
                    long offset = offsets.get(partition).offset();

                    if (offset < lastOffset + 1) {
                        offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
                    }
                } else {
                    offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
                }
            }
        }
    }
}