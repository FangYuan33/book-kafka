package config.policy;

import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy;

import java.util.Map;

/**
 * 创建主题时用该类进行主题参数校验
 *
 * @author FangYuan
 * @since 2023-03-05 10:55:26
 */
public class MyCreateTopicPolicy implements CreateTopicPolicy {
    @Override
    public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {
        if (requestMetadata.numPartitions() != null || requestMetadata.replicationFactor() != null) {
            // 分区数不得小于3
            if (requestMetadata.numPartitions() < 3) {
                throw new PolicyViolationException("Topic should have at " + "least 3 partitions, received: "+
                        requestMetadata.numPartitions());
            }

            // 副本因子必须大于1
            if (requestMetadata.replicationFactor() <= 1) {
                throw new PolicyViolationException("Topic should have at " + "least 2 replication factor, received: "+
                        requestMetadata.replicationFactor());
            }
        }
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
