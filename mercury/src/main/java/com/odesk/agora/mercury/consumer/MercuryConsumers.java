package com.odesk.agora.mercury.consumer;

import com.odesk.agora.mercury.MercuryMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;


/**
 * Created by Dmitry Solovyov on 12/25/2015.
 * <p>
 * A registry of consumers for Mercury topics. Each topic can have only one "main consumer" and only one "DLQ consumer".
 * The main consumer is used for normal message processing after polling from the subscription queue.
 * The DLQ consumer processes the messages that failed to be processed by the "main" one and were moved to the dead letter queue.
 * <p>
 * Each consumer is just an instance of {@link java.util.function.Consumer}. It should process the message silently or throw any RuntimeException if the processing fails.
 * <p>
 * It is the main public API point (facade) at consumer side.
 */
public class MercuryConsumers {
    private static final Logger logger = LoggerFactory.getLogger(MercuryConsumers.class);

    @FunctionalInterface
    public interface DeserializerForClass<T> {
        /**
         * Deserialize the given payload string of the given content-type, when the java class of the payload object is known.
         */
        T deserialize(String serializedPayload, String contentType);
    }

    private static final ConcurrentHashMap<String, Consumer<MercuryMessage>> consumers = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Consumer<MercuryMessage>> dlqConsumers = new ConcurrentHashMap<>();


    /**
     * Get a main/DLQ consumer registered for the given topic.
     */
    public static Consumer<MercuryMessage> getConsumerForTopic(String topicName, boolean dlq) {
        return (dlq ? dlqConsumers : consumers).get(topicName);
    }


    /**
     * Set a main consumer for the given topic.
     * This way you could register a consumer that operates on the untyped {@link MercuryMessage} with raw {@link MercuryMessage#serializedPayload} string
     *  and performs some custom deserialization on his own.
     * <p>
     * Note: For most cases it's more convenient to rely on the automatic deserialization provided by the {@link MercuryDeserializers} registry
     *  and use a typed consumer that operates on the {@link TypedMessage}: {@link #setTypedConsumer(String, Class, Consumer)}
     */
    public static void setConsumer(String topicName, Consumer<MercuryMessage> consumer) {
        consumers.put(topicName, consumer);
        logger.info("Registered a consumer for topic {}: {}", topicName, consumer);
    }

    /**
     * Set a deserializing main consumer for the given topic.
     * This way you could register a typed consumer that operates on the {@link TypedMessage}, alongside with the {@link DeserializerForClass} being used for payload deserialization.
     * So, the raw message is first deserialized with the given deserializer, and then passed to the given consumer.
     * <p>
     * Use this method when T class requires some tricky deserialization that is easier to be coded as
     *  {@link DeserializerForClass#deserialize(String, String)} rather than {@link MercuryDeserializers.DeserializerForContentType#deserialize(String, Class)}.
     * For most other cases it's more convenient to rely on the automatic deserialization provided by the {@link MercuryDeserializers} registry
     *  and use {@link #setTypedConsumer(String, Class, Consumer)}.
     * <p>
     * NB: if you register a consumer this way, then all messages in the given topic should contain a payload of the same (or assignable) class.
     * Nevertheless they still could be transferred in different content-types.
     */
    public static <T> void setDeserializingConsumer(String topicName, DeserializerForClass<T> deserializer, Consumer<TypedMessage<T>> consumer) {
        setConsumer(topicName, (message) -> consumer.accept(new TypedMessage<T>(message, deserializer.deserialize(message.getSerializedPayload(), message.getContentType()))));
    }

    /**
     * Set a typed main consumer for the given topic.
     * This way you could register a typed consumer that operates on the {@link TypedMessage} of the given payloadClass.
     * So, the raw message is first automatically deserialized using the {@link MercuryDeserializers} registry, and then passed to the given consumer.
     * <p>
     * This method of consumers registration match most common use-cases.
     * <p>
     * NB: if you register a consumer this way, then all messages in the given topic should contain a payload of the same (or assignable) class.
     * Nevertheless they still could be transferred in different content-types.
     */
    public static <T> void setTypedConsumer(String topicName, Class<T> payloadClass, Consumer<TypedMessage<T>> consumer) {
        setDeserializingConsumer(topicName, (serializedPayload, contentType) -> MercuryDeserializers.deserialize(serializedPayload, contentType, payloadClass), consumer);
    }

    /**
     * Set a DLQ consumer for the given topic.
     * @see #setConsumer(String, Consumer) the same method for subscription queue
     */
    public static void setDlqConsumer(String topicName, Consumer<MercuryMessage> consumer) {
        dlqConsumers.put(topicName, consumer);
        logger.info("Registered a DLQ consumer for topic {}: {}", topicName, consumer);
    }

    /**
     * Set a deserializing DLQ consumer for the given topic.
     * @see #setDeserializingConsumer(String, DeserializerForClass, Consumer) the same method for subscription queue
     */
    public static <T> void setDlqDeserializingConsumer(String topicName,DeserializerForClass<T> deserializer, Consumer<TypedMessage<T>> consumer) {
        setDlqConsumer(topicName, (message) -> consumer.accept(new TypedMessage<T>(message, deserializer.deserialize(message.getSerializedPayload(), message.getContentType()))));
    }

    /**
     * Set a typed DLQ consumer for the given topic.
     * @see #setTypedConsumer(String, Class, Consumer) the same method for subscription queue
     */
    public static <T> void setDlqTypedConsumer(String topicName, Class<T> payloadClass, Consumer<TypedMessage<T>> consumer) {
        setDlqDeserializingConsumer(topicName, (serializedPayload, contentType) -> MercuryDeserializers.deserialize(serializedPayload, contentType, payloadClass), consumer);
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
