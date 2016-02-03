package com.odesk.agora.mercury.publisher;

import com.amazonaws.util.json.Jackson;
import com.odesk.agora.mercury.MercuryMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Dmitry Solovyov on 01/18/2016.
 * <p>
 * A registry of serializers to be used for message payload serialization.
 * <p>
 * Each serializer is registered with two parameters: a content-type and a java class (or interface).
 * It means that this serializer would be used to serialize a payload of a specified class (and descendants) to a specified content-type.
 * One can register another serializer for some subclass. Then all instances of this subclass (and its descendants) would be serialized with the new serializer instead of the "parent" one.
 * <p>
 * Default "application/json" serializer for Object.class is registered automatically at startup.
 * Default "application/x-thrift+json" serializer for TBase.class is registered automatically at startup by Agora core.
 * Register a new serializer here if you want to publish the messages of some other content-type.
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

    /**
     * Register a serializer for the specified class and content-type.
     */
    public static <T> void setSerializer(Class<T> clazz, String contentType, Serializer<T> serializer) {
        serializersByContentType.computeIfAbsent(contentType, (ct) -> new ConcurrentHashMap<>()).put(clazz, serializer);
        logger.info("Registered a serializer for class {} and contentType {}: {}", clazz, contentType, serializer);
    }

    /**
     * Shorthand for {@code setSerializer(clazz, "application/json", serializer)}
     */
    public static <T> void setJsonSerializer(Class<T> clazz, Serializer<T> serializer) {
        setSerializer(clazz, MercuryMessage.CONTENT_TYPE_JSON, serializer);
    }

    /**
     * Shorthand for {@code setSerializer(clazz, "application/x-thrift+json", serializer)}
     */
    public static <T> void setThriftSerializer(Class<T> clazz, Serializer<T> serializer) {
        setSerializer(clazz, MercuryMessage.CONTENT_TYPE_THRIFT_JSON, serializer);
    }

    /**
     * Serialize the given payload object to the given content-type.
     * @throws IllegalStateException if a proper serializer could not be found in the registry.
     */
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
