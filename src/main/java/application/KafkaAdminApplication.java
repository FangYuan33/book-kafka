package application;

import config.KafkaConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class KafkaAdminApplication {
    public static void main(String[] args) {
        AdminClient kafkaAdmin = AdminClient.create(KafkaConfig.getAdminProperties());

        // 创建主题，分区数为4 副本因子为3
//        NewTopic newTopic = new NewTopic("test-admin", 4, (short) 3);
//        createTopic(kafkaAdmin, newTopic);

        describeTopic(kafkaAdmin, "test-admin");

//        updateTopicConfig(kafkaAdmin, "test-admin");

//        describeTopicConfig(kafkaAdmin, "test-admin");

//        increaseTopicPartition(kafkaAdmin, "test-admin");
    }

    /**
     * 增加主题分区数
     */
    public static void increaseTopicPartition(AdminClient kafkaAdmin, String topic) {
        NewPartitions increasePartition = NewPartitions.increaseTo(5);
        HashMap<String, NewPartitions> increaseTopicPartitionMap = new HashMap<>();
        increaseTopicPartitionMap.put(topic, increasePartition);

        try {
            kafkaAdmin.createPartitions(increaseTopicPartitionMap).all().get();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaAdmin.close();
        }
    }

    /**
     * 修改主题配置信息
     */
    public static void updateTopicConfig(AdminClient kafkaAdmin, String topic) {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);

        ConfigEntry entry = new ConfigEntry("cleanup.policy", "compact");
        Config config = new Config(Collections.singleton(entry));

        // update config map key: configResource value: 要修改的配置信息
        HashMap<ConfigResource, Config> updateMap = new HashMap<>();
        updateMap.put(configResource, config);

        try {
            kafkaAdmin.alterConfigs(updateMap).all().get();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaAdmin.close();
        }
    }

    /**
     * 获取主题配置信息
     */
    public static void describeTopicConfig(AdminClient kafkaAdmin, String topic) {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        DescribeConfigsResult describeConfigsResult = kafkaAdmin.describeConfigs(Collections.singleton(configResource));

        try {
            Map<ConfigResource, Config> topicConfigMap = describeConfigsResult.all().get();

            for (Config value : topicConfigMap.values()) {
                log.info("Topic Config Info {}", value);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        } finally {
            kafkaAdmin.close();
        }
    }

    /**
     * 获取主题的信息
     */
    private static void describeTopic(AdminClient kafkaAdmin, String topic) {
        DescribeTopicsResult describeTopicsResult = kafkaAdmin.describeTopics(Collections.singleton("test-admin"));

        try {
            Map<String, TopicDescription> describeTopic = describeTopicsResult.all().get();

            for (TopicDescription value : describeTopic.values()) {
                log.info("Topic Info {}", value);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        } finally {
            kafkaAdmin.close();
        }
    }

    /**
     * 创建主题
     */
    public static void createTopic(AdminClient kafkaAdmin, NewTopic newTopic) {
        CreateTopicsResult result = kafkaAdmin.createTopics(Collections.singleton(newTopic));

        try {
            result.all().get();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaAdmin.close();
        }
    }
}
