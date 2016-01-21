package com.odesk.agora.mercury.consumer;

import com.amazonaws.util.json.Jackson;
import com.odesk.agora.mercury.MercuryMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Dmitry Solovyov on 01/18/2016.
 */
public class MercuryDeserializers {
    private static final Logger logger = LoggerFactory.getLogger(MercuryDeserializers.class);

    @FunctionalInterface
    public interface DeserializerForClass<T> {
        T deserialize(String serializedPayload, String contentType);
    }

    @FunctionalInterface
    public interface DeserializerForContentType {
        <T> T deserialize(String serializedPayload, Class<T> clazz);
    }

    private static final ConcurrentHashMap<String, DeserializerForContentType> deserializersByContentType = new ConcurrentHashMap<>();

    static {
        setJsonDeserializer(Jackson::fromJsonString);
    }

    public static <T> void setDeserializer(String contentType, DeserializerForContentType deserializer) {
        deserializersByContentType.put(contentType, deserializer);
        logger.info("Registered a deserializer for {}: {}", contentType, deserializer);
    }

    public static <T> void setJsonDeserializer(DeserializerForContentType deserializer) {
        setDeserializer(MercuryMessage.CONTENT_TYPE_JSON, deserializer);
    }

    public static <T> void setThriftDeserializer(DeserializerForContentType deserializer) {
        setDeserializer(MercuryMessage.CONTENT_TYPE_THRIFT_JSON, deserializer);
    }

    public static <T> T deserialize(String serializedPayload, String contentType, Class<T> payloadClass) {
        DeserializerForContentType commonDeserializer = deserializersByContentType.get(contentType);
        if(commonDeserializer == null) {
            throw new IllegalStateException("Can't find a deserializer for " + contentType);
        } else {
            return commonDeserializer.deserialize(serializedPayload, payloadClass);
        }
    }
}
