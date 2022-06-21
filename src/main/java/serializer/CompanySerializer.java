package serializer;

import com.alibaba.fastjson.JSON;
import domain.Company;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Slf4j
public class CompanySerializer implements Serializer<Company> {

    /**
     * 这个方法不重写默认编码值为 UTF-8
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Company data) {
        try {
            if (data == null) {
                return null;
            }

            return JSON.toJSONString(data).getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new byte[0];
    }

    /**
     * 一般情况下 close() 是一个空方法，如果实现了此方法，
     * 则必须确保此方法的幂等性，因为这个方法很可能会被 KafkaProducer 调用多次
     */
    @Override
    public void close() {

    }
}
