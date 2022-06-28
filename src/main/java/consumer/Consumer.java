package consumer;

import config.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 消费者
 */
public class Consumer<K, V> {

    private final KafkaConsumer<K, V> consumer = new KafkaConsumer<>(KafkaConfig.getConsumerProperties());

    public Consumer(String topic) {
        // consumer只能调用一次这个订阅方法，如果之后再调用，则以后调用的订阅内容为准
//        consumer.subscribe(Collections.singletonList(topic));
        // 正则表达式匹配
//        consumer.subscribe(Pattern.compile("topic-*"));
        // 指定分区1的订阅方法
        consumer.assign(Collections.singletonList(new TopicPartition("topic-demo", 1)));
    }

    public ConsumerRecords<K, V> poll(Duration duration) {
        return consumer.poll(duration);
    }

    /**
     * 想要知道主题的分区信息，可以调用这个方法
     */
    public List<PartitionInfo> partitionsFor(String topic) {
        return consumer.partitionsFor(topic);
    }

    /**
     * 同步提交消费位移
     */
    public void commitSync() {
        consumer.commitSync();
    }

    /**
     * 获取指定分区的最新消费位移
     */
    public OffsetAndMetadata committed(TopicPartition topicPartition) {
        return consumer.committed(topicPartition);
    }

    /**
     * 获取下一条需要消费的消息的位移
     */
    public long position(TopicPartition topicPartition) {
        return consumer.position(topicPartition);
    }

    /**
     * 去掉订阅，包括取消assign的订阅
     */
    public void unsubscribe() {
        consumer.unsubscribe();
    }
}
