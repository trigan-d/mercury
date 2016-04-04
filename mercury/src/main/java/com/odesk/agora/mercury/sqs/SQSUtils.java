package com.odesk.agora.mercury.sqs;

import com.odesk.agora.mercury.consumer.config.ConsumerConfiguration;
import com.odesk.agora.mercury.consumer.config.SubscriptionId;

/**
 * Created by Dmitry Solovyov on 04/04/2016.
 */
public class SQSUtils {
    public static final String QUEUE_NAME_DELIMITER = "-";
    public static final String DLQ_NAME_POSTFIX = "DLQ";

    public static String getFullSQSQueueName(ConsumerConfiguration consumerConfig, SubscriptionId subscriptionId) {
        return consumerConfig.getQueueNamesPrefix() + QUEUE_NAME_DELIMITER + subscriptionId;
    }

    public static String getFullSQSQueueNameForDLQ(ConsumerConfiguration consumerConfig, SubscriptionId subscriptionId) {
        return consumerConfig.getQueueNamesPrefix() + QUEUE_NAME_DELIMITER + subscriptionId + QUEUE_NAME_DELIMITER + DLQ_NAME_POSTFIX;
    }
}
