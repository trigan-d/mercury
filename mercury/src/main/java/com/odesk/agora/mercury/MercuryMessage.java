package com.odesk.agora.mercury;

import com.odesk.agora.mercury.consumer.MercuryDeserializers;
import com.odesk.agora.mercury.publisher.MercurySerializers;
import com.odesk.agora.mercury.publisher.TopicPublisher;
import com.odesk.agora.mercury.publisher.TopicPublishersFactory;

import java.util.Date;
import java.util.Map;

/**
 * Created by Dmitry Solovyov on 11/30/2015.
 * <p>
 * DTO envelope class for Mercury messages.
 * Should not be instantiated directly. At consumer side it is automatically read from SQS message.
 * At publisher side one should use one of "messageWith..." methods from {@link TopicPublisher}
 */
public class MercuryMessage {
    public static final String CONTENT_TYPE_PLAIN = "text/plain";
    public static final String CONTENT_TYPE_JSON = "application/json";
    public static final String CONTENT_TYPE_THRIFT_JSON = "application/x-thrift+json";

    /**
     * The content-type of the serialized payload. Mercury ecosystem supports the following types out of the box: "text/plain", "application/json", "application/x-thrift+json".
     * <p>
     * One could easily define and use any additional content-type.
     * It requires a registration of proper serializers at publisher side with {@link MercurySerializers#setSerializer(Class, String, MercurySerializers.Serializer)},
     * and proper deserializer at consumer side with {@link MercuryDeserializers#setDeserializer(String, MercuryDeserializers.DeserializerForContentType)}
     */
    private String contentType;

    /**
     * The payload being serialized with to the given content-type
     */
    private String serializedPayload;

    /**
     * The ID of Mercury message. Mercury does not require the ID to be unique for a single topic or for the universe.
     * One can set the ID using {@link TopicPublisher.MessageToPublish#withMessageId(String)}.
     * If the ID was not set manually then it would be generated automatically by {@link TopicPublishersFactory#messageIdSupplier} before publishing.
     */
    private String messageId;

    /**
     * The name of Mercury topic the message was published to / consumed from.
     * Is set automatically by {@link TopicPublisher}.
     */
    private String topicName;

    /**
     * The name of Mercury topic the message was published to / consumed from.
     * Is set automatically by {@link TopicPublisher}.
     * The actual value comes from {@link TopicPublishersFactory#senderAppId}. Agora core automatically places the name of publisher service there.
     */
    private String senderAppId;

    /**
     * The timestamp of messsage publication.
     * Is set automatically during {@link TopicPublisher.MessageToPublish#publish()}.
     */
    private Date timestamp;

    /**
     * Any custom user-defined metadata. Null by default.
     * One can fill it using {@link TopicPublisher.MessageToPublish#addMetadata(Map)} or {@link TopicPublisher.MessageToPublish#addMetadata(String, String)}.
     */
    private Map<String, String> metadata;


    public String getSerializedPayload() {
        return serializedPayload;
    }

    public void setSerializedPayload(String serializedPayload) {
        this.serializedPayload = serializedPayload;
    }

    public String getSenderAppId() {
        return senderAppId;
    }

    public void setSenderAppId(String senderAppId) {
        this.senderAppId = senderAppId;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    public String getMetadata(String key) {
        return metadata == null ? null : metadata.get(key);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");

        sb.append("topicName=" + topicName + ", ");
        sb.append("senderAppId=" + senderAppId + ", ");
        sb.append("messageId=" + messageId + ", ");
        sb.append("timestamp=" + timestamp + ", ");
        sb.append("contentType=" + contentType + ", ");
        sb.append("payloadSize=" + serializedPayload.length());

        if(! (metadata == null || metadata.isEmpty()))
        {
            sb.append(", metadata: " + metadata);
        }

        sb.append("}");
        return sb.toString();
    }
}
