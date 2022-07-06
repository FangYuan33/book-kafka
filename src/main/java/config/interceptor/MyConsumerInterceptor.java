package config.interceptor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class MyConsumerInterceptor<K, V> implements ConsumerInterceptor<K, V> {

    private static final long EXPIRE_INTERVAL = 10 * 1000;

    /**
     * 会在poll方法返回消息之前执行，随意更改的啦
     *
     * 延迟时间超过10s的消息会被过滤掉
     */
    @Override
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        long now = System.currentTimeMillis();
        
        Map<TopicPartition, List<ConsumerRecord<K, V>>> newRecords = new HashMap<>();
        for (TopicPartition tp : records.partitions()) {
            List<ConsumerRecord<K, V>> tpRecords = records.records(tp);
            List<ConsumerRecord<K, V>> newTpRecords = new ArrayList<>();

            for (ConsumerRecord<K, V> record : tpRecords) {
                if (record.timestamp() - now > EXPIRE_INTERVAL) {
                    newTpRecords.add(record);
                } else {
                    log.info("record: {} 被过滤掉了", record.value());
                }
            }

            if (!newTpRecords.isEmpty()) {
                newRecords.put(tp, newTpRecords);
            }
        }

        // 为了执行这个构造方法，把消息都分了区
        return new ConsumerRecords<>(newRecords);
    }

    /**
     * 在提交完消费位移之后调用，可以使用这个方法来记录跟踪所提交的位移信息
     */
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        for (TopicPartition topicPartition : offsets.keySet()) {
            OffsetAndMetadata metadata = offsets.get(topicPartition);

//            log.info("Topic: {}, partition: {}, offset: {}",
//                    topicPartition.topic(), topicPartition.partition(), metadata.offset());
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
