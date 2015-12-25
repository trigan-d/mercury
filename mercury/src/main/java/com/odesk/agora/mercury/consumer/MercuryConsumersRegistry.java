package com.odesk.agora.mercury.consumer;

import com.odesk.agora.mercury.MercuryMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Created by Dmitry Solovyov on 12/25/2015.
 */
public class MercuryConsumersRegistry {
    private static final Logger logger = LoggerFactory.getLogger(MercuryConsumersRegistry.class);

    private static final ConcurrentHashMap<String, Consumer<MercuryMessage>> consumers = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Consumer<MercuryMessage>> dlqConsumers = new ConcurrentHashMap<>();

    public static void setTopicConsumer(String topicName, Consumer<MercuryMessage> consumer) {
        consumers.put(topicName, consumer);
        logger.info("Registered a consumer for topic {}: {}", topicName, consumer);
    }

    public static void removeTopicConsumer(String topicName) {
        consumers.remove(topicName);
        logger.info("Consumer for topic {} unregistered", topicName);
    }

    public static void setTopicDlqConsumer(String topicName, Consumer<MercuryMessage> consumer) {
        dlqConsumers.put(topicName, consumer);
        logger.info("Registered a DLQ consumer for topic {}: {}", topicName, consumer);
    }

    public static void removeTopicDlqConsumer(String topicName) {
        dlqConsumers.remove(topicName);
        logger.info("DLQ consumer for topic {} unregistered", topicName);
    }

    public static Consumer<MercuryMessage> getConsumerForTopic(String topicName, boolean dlq) {
        return (dlq ? dlqConsumers : consumers).get(topicName);
    }
}
