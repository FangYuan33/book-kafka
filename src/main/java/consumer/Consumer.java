package consumer;

import config.KafkaConfig;
import config.rebalance.MyRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 消费者
 */
public class Consumer<K, V> {

    /**
     * 还是开放出来一个consumer供自己用吧
     */
    public KafkaConsumer<K, V> getConsumer() {
        return consumer;
    }

    private final KafkaConsumer<K, V> consumer = new KafkaConsumer<>(KafkaConfig.getConsumerProperties());

    public Consumer(String topic) {
        // consumer只能调用一次这个订阅方法，如果之后再调用，则以后调用的订阅内容为准
        consumer.subscribe(Collections.singletonList(topic), new MyRebalanceListener<>(consumer));
        // 正则表达式匹配
//        consumer.subscribe(Pattern.compile("topic-*"));
        // 指定分区1的订阅方法
//        consumer.assign(Collections.singletonList(new TopicPartition("topic-demo", 1)));
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
     * 提交特定话题特定分区的消费位移，但是一般不会消费完一条就调用一次这个方法
     */
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        consumer.commitSync(offsets);
    }

    /**
     * 异步提交
     */
    public void commitAsync() {
        consumer.commitAsync((offsets, exception) -> {
            // 异步提交的回调方法

        });
    }

    /**
     * 暂停分区消费
     */
    public void pause(TopicPartition topicPartition) {
        consumer.pause(Collections.singleton(topicPartition));
    }

    /**
     * 恢复指定分区消费
     */
    public void resume(TopicPartition topicPartition) {
        consumer.resume(Collections.singleton(topicPartition));
    }

    /**
     * 获取所有暂停消费的分区
     */
    public Set<TopicPartition> paused() {
        return consumer.paused();
    }

    /**
     * 调用 wakeup() 方法后可以退出 poll() 的逻辑，并抛出 WakeupException 的异常
     *
     * 唯一可以从其他线程里安全调用的方法
     */
    public void wakeup() {
        consumer.wakeup();
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

    /**
     * 关闭消费者客户端
     */
    public void close() {
        consumer.close();
    }
}
