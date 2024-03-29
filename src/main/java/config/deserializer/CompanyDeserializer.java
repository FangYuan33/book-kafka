package config.deserializer;

import com.alibaba.fastjson.JSONObject;
import domain.Company;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * 消费者需要用反序列化器（Deserializer）把从 Kafka 中收到的字节数组转换成相应的对象
 */
@Slf4j
public class CompanyDeserializer implements Deserializer<Company> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Company deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                return null;
            }

            String jsonCompany = new String(data, StandardCharsets.UTF_8);

            return JSONObject.parseObject(jsonCompany, Company.class);
        } catch (Exception e) {
            log.error("company反解析异常", e);
        }

        return null;
    }

    @Override
    public void close() {

    }
}
