package com.odesk.agora.mercury.publisher;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.util.StringUtils;
import com.amazonaws.util.json.Jackson;
import com.odesk.agora.mercury.MercuryMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Created by Dmitry Solovyov on 11/23/2015.
 */
public class TopicPublisher {
    private final Logger logger;

    private final AmazonSNSClient snsClient;
    private final String topicName;
    private final String topicArn;
    private final String senderAppId;
    private Supplier<String> messageIdSupplier;

    public TopicPublisher(AmazonSNSClient snsClient, String topicName, String topicArn, String senderAppId, Supplier<String> messageIdSupplier) {
        this.snsClient = snsClient;
        this.topicName = topicName;
        this.topicArn = topicArn;
        this.senderAppId = senderAppId;
        this.messageIdSupplier = messageIdSupplier;

        logger = LoggerFactory.getLogger(TopicPublisher.class + "-" + topicName);
    }

    public MessageToPublish messageWithSerializedPayload(String serializedPayload, String contentType) {
        return new MessageToPublish(serializedPayload, contentType);
    }

    public MessageToPublish messageWithTextPayload(String value) {
        return messageWithSerializedPayload(value, MercuryMessage.CONTENT_TYPE_PLAIN);
    }

    public <T> MessageToPublish messageWithObjectPayload(T value, String contentType) {
        return messageWithSerializedPayload(MercurySerializers.serialize(value, contentType), contentType);
    }

    public <T> MessageToPublish messageWithThriftPayload(T value) {
        return messageWithObjectPayload(value, MercuryMessage.CONTENT_TYPE_THRIFT_JSON);
    }

    public <T> MessageToPublish messageWithJsonPayload(T value) {
        return messageWithObjectPayload(value, MercuryMessage.CONTENT_TYPE_JSON);
    }


    public class MessageToPublish extends MercuryMessage {
        private MessageToPublish(String serializedPayload, String contentType) {
            setSerializedPayload(serializedPayload);
            setContentType(contentType);

            setSenderAppId(senderAppId);
            setTopicName(topicName);
        }

        public MessageToPublish withMessageId(String messageId) {
            setMessageId(messageId);
            return this;
        }

        public MessageToPublish addMetadata(Map<String, String> metadata) {
            setMetadata(metadata);
            return this;
        }

        public MessageToPublish addMetadata(String key, String value) {
            if(getMetadata() == null) {
                setMetadata(new HashMap<>());
            }
            getMetadata().put(key,value);
            return this;
        }

        public String publish() {
            if(StringUtils.isNullOrEmpty(getMessageId())) {
                setMessageId(messageIdSupplier.get());
            }
            setTimestamp(new Date());
            PublishRequest request = new PublishRequest(topicArn, Jackson.toJsonString(this));
            logger.debug("Publishing message {}", this);
            return snsClient.publish(request).getMessageId();
        }
    }
}