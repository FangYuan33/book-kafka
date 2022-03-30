package test;

import admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Test20 {
}

class AdminClient20 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        AdminClient adminClient = KafkaAdminClient.getAdminClient();

        // 创建一个新的主题 三个分区 一个副本因子
        NewTopic topic = new NewTopic("admin-client-demo", 3, (short) 1);
        CreateTopicsResult result = adminClient.createTopics(Collections.singleton(topic));

        // 遍历主题名字
        for (String name : adminClient.listTopics().names().get()) {
            System.out.println(name);
        }

        Map<String, TopicDescription> topicDescriptionMap = adminClient
                .describeTopics(Collections.singleton("admin-client-demo")).allTopicNames().get();
        for (Map.Entry<String, TopicDescription> entry : topicDescriptionMap.entrySet()) {
            System.out.println(entry.toString());
        }

        // 获取配置
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, "admin-client-demo");
        Map<ConfigResource, Config> configMap = adminClient
                .describeConfigs(Collections.singleton(configResource)).all().get();

        for (Map.Entry<ConfigResource, Config> entry : configMap.entrySet()) {
            System.out.println(entry.toString());
        }

        // 修改配置
        ConfigEntry entry = new ConfigEntry("cleanup.policy", "compact");
        AlterConfigOp alterConfigOp = new AlterConfigOp(entry, AlterConfigOp.OpType.SET);
        Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
        configs.put(configResource, Collections.singleton(alterConfigOp));

        adminClient.incrementalAlterConfigs(configs).all().get();

        // 删除主题
        adminClient.deleteTopics(Collections.singleton("admin-client-demo"));

        result.all().get();
        adminClient.close();
    }
}
