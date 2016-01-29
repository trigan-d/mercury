package com.odesk.agora.mercury.consumer;

import com.amazonaws.util.json.Jackson;
import com.odesk.agora.mercury.MercuryMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Dmitry Solovyov on 01/18/2016.
 * <p>
 * A registry of {@link DeserializerForContentType} deserializers to be used for message payload deserialization.
 * Each deserializer is registered to be used for the payloads of some specific content-type.
 * <p>
 * Default "application/json" deserializer is registered automatically at startup.
 * Default "application/x-thrift+json" deserializer is registered automatically at startup by Agora core.
 * Register a new deserializer here if you want to receive and process the messages of some other content-type.
 */
public class MercuryDeserializers {
    private static final Logger logger = LoggerFactory.getLogger(MercuryDeserializers.class);

    @FunctionalInterface
    public interface DeserializerForContentType {
        /**
         * Deserialize the given payload object of the given java class, when the serialization content-type is known.
         */
        <T> T deserialize(String serializedPayload, Class<T> clazz);
    }

    private static final ConcurrentHashMap<String, DeserializerForContentType> deserializersByContentType = new ConcurrentHashMap<>();

    static {
        setJsonDeserializer(Jackson::fromJsonString);
    }

    /**
     * Register a deserializer for the given content-type.
     */
    public static void setDeserializer(String contentType, DeserializerForContentType deserializer) {
        deserializersByContentType.put(contentType, deserializer);
        logger.info("Registered a deserializer for {}: {}", contentType, deserializer);
    }

    /**
     * Shorthand for {@code setDeserializer("application/json", serializer)}
     */
    public static void setJsonDeserializer(DeserializerForContentType deserializer) {
        setDeserializer(MercuryMessage.CONTENT_TYPE_JSON, deserializer);
    }

    /**
     * Shorthand for {@code setDeserializer("application/x-thrift+json", serializer)}
     */
    public static void setThriftDeserializer(DeserializerForContentType deserializer) {
        setDeserializer(MercuryMessage.CONTENT_TYPE_THRIFT_JSON, deserializer);
    }

    /**
     * Deserialize the given payload of the given content-type to the object of a given java class.
     * @throws IllegalStateException if a proper deserializer could not be found in the registry.
     */
    public static <T> T deserialize(String serializedPayload, String contentType, Class<T> payloadClass) {
        DeserializerForContentType deserializer = deserializersByContentType.get(contentType);
        if(deserializer == null) {
            throw new IllegalStateException("Can't find a deserializer for " + contentType);
        } else {
            return deserializer.deserialize(serializedPayload, payloadClass);
        }
    }
}
