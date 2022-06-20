package serializer;

import domain.Company;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
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
        if (data == null) {
            return null;
        }

        byte[] name, address;

        try {
            if (data.getName() != null) {
                name = data.getName().getBytes(StandardCharsets.UTF_8);
            } else {
                name = new byte[0];
            }

            if (data.getAddress() != null) {
                address = data.getAddress().getBytes(StandardCharsets.UTF_8);
            } else {
                address = new byte[0];
            }
            // 两个4代表两次putInt 每个int 4个字节
            ByteBuffer result = ByteBuffer.allocate(4 + 4 + name.length + address.length);
            result.putInt(name.length);
            result.put(name);
            result.putInt(address.length);
            result.put(address);

            return result.array();
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
