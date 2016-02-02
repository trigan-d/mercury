package com.odesk.agora.mercury.consumer;

import com.odesk.agora.mercury.MercuryMessage;
import com.amazonaws.services.sqs.model.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;


/**
 * Created by Dmitry Solovyov on 02/02/2016.
 * <p>
 * A registry of plain SQS consumers for Mercury topics. Acts much like {@link MercuryConsumers}, but intended for low-level SQS messages processing
 * without an attempt to extract {@link MercuryMessage}. So all the consumers registered here operate on {@link Message} directly.
 * Those consumers always take precedence over the ones registered at {@link MercuryConsumers} for the same topics.
 */
public class PlainSQSConsumers {
    private static final Logger logger = LoggerFactory.getLogger(PlainSQSConsumers.class);

    private static final ConcurrentHashMap<String, Consumer<Message>> consumers = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Consumer<Message>> dlqConsumers = new ConcurrentHashMap<>();

    /**
     * Get a main/DLQ consumer registered for the given topic.
     */
    public static Consumer<Message> getConsumerForTopic(String topicName, boolean dlq) {
        return (dlq ? dlqConsumers : consumers).get(topicName);
    }

    /**
     * Set a main consumer for the given topic.
     */
    public static void setConsumer(String topicName, Consumer<Message> consumer) {
        consumers.put(topicName, consumer);
        logger.info("Registered a consumer for topic {}: {}", topicName, consumer);
    }

    /**
     * Set a DLQ consumer for the given topic.
     * @see #setConsumer(String, Consumer) the same method for subscription queue
     */
    public static void setDlqConsumer(String topicName, Consumer<Message> consumer) {
        dlqConsumers.put(topicName, consumer);
        logger.info("Registered a DLQ consumer for topic {}: {}", topicName, consumer);
    }

    /**
     * Remove a main consumer for the given topic
     */
    public static void removeConsumer(String topicName) {
        consumers.remove(topicName);
        logger.info("Consumer for topic {} unregistered", topicName);
    }

    /**
     * Remove a DLQ consumer for the given topic
     */
    public static void removeDlqConsumer(String topicName) {
        dlqConsumers.remove(topicName);
        logger.info("DLQ consumer for topic {} unregistered", topicName);
    }
}
