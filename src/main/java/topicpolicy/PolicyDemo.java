package topicpolicy;

import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy;

import java.util.Map;

/**
 * 这狗东西需要在broker所在的服务器上一起编译才有效
 */
public class PolicyDemo implements CreateTopicPolicy {
    @Override
    public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {
        if (requestMetadata.numPartitions() != null && requestMetadata.numPartitions() > 10) {
            throw new PolicyViolationException("Topic partitions num should not more than 10, provide value: "
                    + requestMetadata.numPartitions());
        }
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
