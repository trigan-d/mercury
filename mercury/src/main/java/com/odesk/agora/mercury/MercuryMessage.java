package com.odesk.agora.mercury;

import java.util.Date;

/**
 * Created by Dmitry Solovyov on 11/30/2015.
 */
public class MercuryMessage {
    public static final String CONTENT_TYPE_PLAIN = "text/plain";
    public static final String CONTENT_TYPE_JSON = "application/json";
    public static final String CONTENT_TYPE_THRIFT_JSON = "application/x-thrift+json";

    private String contentType;
    private String serializedPayload;

    private String messageId;

    private String topicName;
    private String senderAppId;
    private Date timestamp;

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
}
