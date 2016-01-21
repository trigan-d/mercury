package com.odesk.agora.mercury.publisher;

import com.amazonaws.util.json.Jackson;
import com.odesk.agora.mercury.MercuryMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Dmitry Solovyov on 01/18/2016.
 */
public class MercurySerializers {
    @FunctionalInterface
    public interface Serializer<T> {
        String serialize(T value);
    }

    private static final Logger logger = LoggerFactory.getLogger(MercurySerializers.class);

    private static final ConcurrentHashMap<String, ConcurrentHashMap<Class, Serializer>> serializersByContentType = new ConcurrentHashMap<>();

    static {
        setJsonSerializer(Object.class, Jackson::toJsonString);
    }

    public static <T> void setSerializer(Class<T> clazz, String contentType, Serializer<T> serializer) {
        serializersByContentType.computeIfAbsent(contentType, (ct) -> new ConcurrentHashMap<>()).put(clazz, serializer);
        logger.info("Registered a serializer for class {} and contentType {}: {}", clazz, contentType, serializer);
    }

    public static <T> void setJsonSerializer(Class<T> clazz, Serializer<T> serializer) {
        setSerializer(clazz, MercuryMessage.CONTENT_TYPE_JSON, serializer);
    }

    public static <T> void setThriftSerializer(Class<T> clazz, Serializer<T> serializer) {
        setSerializer(clazz, MercuryMessage.CONTENT_TYPE_THRIFT_JSON, serializer);
    }

    public static <T> String serialize(T value, String contentType) {
        Serializer<T> serializer = null;
        ConcurrentHashMap<Class, Serializer> serializersByClass = serializersByContentType.get(contentType);

        if(serializersByClass != null && !serializersByClass.isEmpty()) {
            serializer = findProperSerializerForClass(value.getClass(), serializersByClass);
        }

        if(serializer == null) {
            throw new IllegalStateException("Can't find a proper " + contentType + " serializer for " + value.getClass().getName());
        } else {
            return serializer.serialize(value);
        }
    }

    private static <T> Serializer<T> findProperSerializerForClass(Class clazz, ConcurrentHashMap<Class, Serializer> serializersByClass) {
        if(clazz == null) {
            return null;
        }

        Serializer<T> serializer = serializersByClass.get(clazz);

        if(serializer == null) {
            serializer = findProperSerializerForClass(clazz.getSuperclass(), serializersByClass);
        }

        if(serializer == null) {
            for(Class iClass : clazz.getInterfaces()) {
                serializer = findProperSerializerForClass(iClass, serializersByClass);

                if(serializer != null) {
                    break;
                }
            }
        }

        return serializer;
    }
}
