package com.odesk.agora.mercury.consumer;

import com.odesk.agora.mercury.MercuryMessage;
import com.odesk.agora.mercury.consumer.config.SubscriptionId;

import java.util.function.Consumer;

/**
 * Created by Dmitry Solovyov on 01/20/2016.
 * <p>
 * Type-safe wrapper for Mercury message containing a deserialized version of {@link MercuryMessage#serializedPayload}.
 * Should not be instantiated directly. The instances are created on the fly by the deserializing consumer registered with
 * {@link MercuryConsumers#setDeserializingConsumer(SubscriptionId, MercuryConsumers.DeserializerForClass, Consumer)} or
 * {@link MercuryConsumers#setTypedConsumer(SubscriptionId, Class, Consumer)}.
 */
public class TypedMessage<T> extends MercuryMessage {
    private final T payload;

    protected TypedMessage(MercuryMessage original, T payload) {
        setSerializedPayload(original.getSerializedPayload());
        setContentType(original.getContentType());
        setPayloadType(original.getPayloadType());
        setMessageId(original.getMessageId());
        setTimestamp(original.getTimestamp());
        setSenderAppId(original.getSenderAppId());
        setTopicName(original.getTopicName());
        setMetadata(original.getMetadata());
        setAgoraMDCData(original.getAgoraMDCData());

        this.payload = payload;
    }

    /**
     * @return deserialized payload object
     */
    public T getPayload() {
        return payload;
    }
}
