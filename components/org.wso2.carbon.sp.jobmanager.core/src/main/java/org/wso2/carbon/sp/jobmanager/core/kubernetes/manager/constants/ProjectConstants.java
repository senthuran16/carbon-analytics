package org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.constants;

/**
 * Has Project level constants
 */
public class ProjectConstants {
    private static String kafkaURL;

    /**
     * Prevents instantiation
     */
    private ProjectConstants() {}

    public static String getKafkaURL() {
        return kafkaURL;
    }

    public static void setKafkaURL(String kafkaIp, String kafkaPort) {
        ProjectConstants.kafkaURL = kafkaIp + ":" + kafkaPort;
    }
}
