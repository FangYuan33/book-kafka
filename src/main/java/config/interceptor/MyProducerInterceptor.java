package config.interceptor;

import domain.Company;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 这3个方法中抛出的异常都会被捕获并记录到日志中，但并不会再向上传递
 */
@Slf4j
public class MyProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

    private AtomicInteger allSend = new AtomicInteger(0);

    private AtomicInteger successSend = new AtomicInteger(0);

    /**
     * 在将消息序列化和计算分区之前会调用生产者拦截器的 onSend() 方法来对消息进行相应的定制化操作
     */
    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        if (record.value() instanceof Company) {
            Company value = (Company) record.value();
            if (value != null && value.getName().equals("JD")) {
                value.setName("Dog D?");
            }
        }

        return record;
    }

    /**
     * 在消息被应答（Acknowledgement）之前或消息发送失败时调用生产者拦截器的 onAcknowledgement() 方法
     *
     * 优先于用户设定的 Callback 之前执行
     * 这个方法运行在 Producer 的I/O线程中，所以这个方法中实现的代码逻辑越简单越好，否则会影响消息的发送速度
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        allSend.addAndGet(1);

        if (exception == null) {
            successSend.addAndGet(1);
        }
    }

    /**
     * close() 方法主要用于在关闭拦截器时执行一些资源的清理工作
     */
    @Override
    public void close() {
        int success = successSend.get();
        int all = allSend.get();

        log.info("成功率: {}", BigDecimal.valueOf(success).divide(BigDecimal.valueOf(all), 2, BigDecimal.ROUND_HALF_UP));
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
