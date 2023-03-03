package config.rebalance;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

/**
 * 当主题分区的所属权从一个消费者转移到另一个消费者的行为是再均衡，此时会触发再均衡器的执行逻辑
 */
@Slf4j
public class MyReBalanceListener<K, V> implements ConsumerRebalanceListener {

    private final KafkaConsumer<K, V> consumer;

    public MyReBalanceListener(KafkaConsumer<K, V> consumer) {
        this.consumer = consumer;
    }

    /**
     * 会在再均衡开始之前和消费者停止读取消息之后被调用
     *
     * 在再均衡之前将消费位移提交，避免均衡之后别的消费者分配到该分区发生重复消费
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("触发再均衡器...");
        consumer.commitSync();
    }

    /**
     * 会在重新分配分区之后和消费者开始读取消费之前被调用
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("再均衡器触发完毕，准备开始读取消息...");
    }
}
