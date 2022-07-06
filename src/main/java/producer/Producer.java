package producer;

import config.KafkaConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 消息的发布者
 */
@Slf4j
public class Producer<K, V> {

    private final KafkaProducer<K, V> producer = new KafkaProducer<>(KafkaConfig.getProducerProperties());

    /**
     * 同步发送消息
     */
    public void syncSendMessage(ProducerRecord<K, V> producerRecord) {
        try {
            Future<RecordMetadata> future = producer.send(producerRecord);
            // RecordMetadata包含了一些元信息
            RecordMetadata recordMetadata = future.get();

            log.info("同步消息发送的主题 {}, 分区 {}, 偏移量 {}",
                    recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            log.error("同步发送消息失败: " + e.getMessage());
        }

        // close()方法会阻塞等待之前所有的发送请求完成后再关闭 KafkaProducer
//        producer.close();
    }

    /**
     * 异步发送消息
     */
    public void asyncSendMessage(ProducerRecord<K, V> producerRecord) {
        producer.send(producerRecord, (recordMetadata, exception) -> {
            if (exception != null) {
                log.error("异步发送消息失败: " + exception.getMessage());
            } else {
                log.info("异步消息发送的主题 {}, 分区 {}, 偏移量 {}",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
            }
        });

        producer.close();
    }
}
