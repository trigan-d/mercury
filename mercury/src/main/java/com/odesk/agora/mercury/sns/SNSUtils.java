package com.odesk.agora.mercury.sns;

import com.odesk.agora.mercury.publisher.config.PublisherConfiguration;

/**
 * Created by Dmitry Solovyov on 04/04/2016.
 */
public class SNSUtils {
    public static final String TOPIC_NAME_DELIMITER = "-";

    public static String getFullSNSTopicName(PublisherConfiguration publisherConfig, String topicName) {
        return publisherConfig.getTopicNamesPrefix() + TOPIC_NAME_DELIMITER + topicName;
    }
}
