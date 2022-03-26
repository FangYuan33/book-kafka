package interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 自定义卡夫卡拦截器
 * 发送消息器为value拼接前缀
 * 并通过onAcknowledgement方法计算成功率
 */
public class PrefixProducerInterceptor implements ProducerInterceptor<String, String> {

    private final AtomicLong sendSuccess = new AtomicLong();

    private final AtomicLong sendFailure = new AtomicLong();

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String prefixValue = "fy-" + record.value();

        return new ProducerRecord<>(record.topic(), record.partition(),
                record.timestamp(), record.key(), prefixValue, record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            sendSuccess.addAndGet(1);
        } else {
            sendFailure.addAndGet(1);
        }
    }

    @Override
    public void close() {
        BigDecimal sum = BigDecimal.valueOf(sendSuccess.getAndAdd(sendFailure.get()));

        BigDecimal successRatio = BigDecimal.valueOf(sendSuccess.get()).divide(sum, 2, RoundingMode.HALF_UP);

        System.out.println("消息发送成功率：" + successRatio);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
