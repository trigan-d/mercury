package com.odesk.agora.mercury.consumer;

import com.odesk.agora.mercury.MercuryMessage;

import java.util.function.Consumer;

/**
 * Created by Dmitry Solovyov on 01/20/2016.
 * <p>
 * Type-safe wrapper for Mercury message containing a deserialized version of {@link MercuryMessage#serializedPayload}.
 * Should not be instantiated directly. The instances are created on the fly by the deserializing consumer registered with
 * {@link MercuryConsumers#setDeserializingConsumer(String, MercuryConsumers.DeserializerForClass, Consumer)} or
 * {@link MercuryConsumers#setTypedConsumer(String, Class, Consumer)}.
 */
public class TypedMessage<T> extends MercuryMessage {
    private final T payload;

    protected TypedMessage(MercuryMessage original, T payload) {
        setSerializedPayload(original.getSerializedPayload());
        setContentType(original.getContentType());
        setMessageId(original.getMessageId());
        setTimestamp(original.getTimestamp());
        setSenderAppId(original.getSenderAppId());
        setTopicName(original.getTopicName());
        setMetadata(original.getMetadata());

        this.payload = payload;
    }

    /**
     * @return deserialized payload object
     */
    public T getPayload() {
        return payload;
    }
}
