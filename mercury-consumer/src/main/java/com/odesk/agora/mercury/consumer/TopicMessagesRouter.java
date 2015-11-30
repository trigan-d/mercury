package com.odesk.agora.mercury.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Created by Dmitry Solovyov on 11/27/2015.
 */
public class TopicMessagesRouter {
    private static final Logger logger = LoggerFactory.getLogger(TopicMessagesRouter.class);

    private final ConcurrentHashMap<String, Consumer<MercuryMessage>> consumers = new ConcurrentHashMap<>();

    //TODO: discuss and implement an ability to register several consumers for one topic

    public void setTopicConsumer(String topicName, Consumer<MercuryMessage> consumer) {
        consumers.put(topicName, consumer);
        logger.info("Registered a consumer for topic {}: {}", topicName, consumer);
    }

    public void removeTopicConsumer(String topicName) {
        consumers.remove(topicName);
        logger.info("Consumer for topic {} unregistered", topicName);
    }

    public void route(MercuryMessage message) {
        Consumer<MercuryMessage> consumer = consumers.get(message.getTopicName());
        if(consumer == null) {
            logger.warn("No consumer foud for message {}", message);
            //TODO: what should we do with the messages that were not consumed? Move them to DLQ?
        } else {
            consumer.accept(message);
        }
    }
}
