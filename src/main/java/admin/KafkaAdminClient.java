package admin;

import config.KafkaConfig;
import org.apache.kafka.clients.admin.AdminClient;

public class KafkaAdminClient {
    private static final AdminClient ADMIN_CLIENT;

    static {
        ADMIN_CLIENT = AdminClient.create(KafkaConfig.getAdminProperties());
    }

    public static AdminClient getAdminClient() {
        return ADMIN_CLIENT;
    }
}
