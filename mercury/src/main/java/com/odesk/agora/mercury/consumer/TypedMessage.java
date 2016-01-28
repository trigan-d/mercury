package com.odesk.agora.mercury.consumer;

import com.odesk.agora.mercury.MercuryMessage;

/**
 * Created by Dmitry Solovyov on 01/20/2016.
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

    public T getPayload() {
        return payload;
    }
}
