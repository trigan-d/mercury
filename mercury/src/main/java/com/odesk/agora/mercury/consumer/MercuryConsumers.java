package com.odesk.agora.mercury.consumer;

import com.odesk.agora.mercury.MercuryMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static com.odesk.agora.mercury.consumer.MercuryDeserializers.*;

/**
 * Created by Dmitry Solovyov on 12/25/2015.
 */
public class MercuryConsumers {
    private static final Logger logger = LoggerFactory.getLogger(MercuryConsumers.class);

    private static final ConcurrentHashMap<String, Consumer<MercuryMessage>> consumers = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Consumer<MercuryMessage>> dlqConsumers = new ConcurrentHashMap<>();


    public static Consumer<MercuryMessage> getConsumerForTopic(String topicName, boolean dlq) {
        return (dlq ? dlqConsumers : consumers).get(topicName);
    }


    public static void setConsumer(String topicName, Consumer<MercuryMessage> consumer) {
        consumers.put(topicName, consumer);
        logger.info("Registered a consumer for topic {}: {}", topicName, consumer);
    }

    public static <T> void setDeserializingConsumer(String topicName, DeserializerForClass<T> deserializer, Consumer<TypedMessage<T>> consumer) {
        setConsumer(topicName, (message) -> consumer.accept(new TypedMessage<T>(message, deserializer.deserialize(message.getSerializedPayload(), message.getContentType()))));
    }

    public static <T> void setTypedConsumer(String topicName, Class<T> payloadClass, Consumer<TypedMessage<T>> consumer) {
        setDeserializingConsumer(topicName, (serializedPayload, contentType) -> deserialize(serializedPayload, contentType, payloadClass), consumer);
    }


    public static void setDlqConsumer(String topicName, Consumer<MercuryMessage> consumer) {
        dlqConsumers.put(topicName, consumer);
        logger.info("Registered a DLQ consumer for topic {}: {}", topicName, consumer);
    }

    public static <T> void setDlqDeserializingConsumer(String topicName, DeserializerForClass<T> deserializer, Consumer<TypedMessage<T>> consumer) {
        setDlqConsumer(topicName, (message) -> consumer.accept(new TypedMessage<T>(message, deserializer.deserialize(message.getSerializedPayload(), message.getContentType()))));
    }

    public static <T> void setDlqTypedConsumer(String topicName, Class<T> payloadClass, Consumer<TypedMessage<T>> consumer) {
        setDlqDeserializingConsumer(topicName, (serializedPayload, contentType) -> deserialize(serializedPayload, contentType, payloadClass), consumer);
    }


    public static void removeConsumer(String topicName) {
        consumers.remove(topicName);
        logger.info("Consumer for topic {} unregistered", topicName);
    }

    public static void removeDlqConsumer(String topicName) {
        dlqConsumers.remove(topicName);
        logger.info("DLQ consumer for topic {} unregistered", topicName);
    }
}
