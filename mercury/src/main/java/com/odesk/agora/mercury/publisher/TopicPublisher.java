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
 * <p>
 * Publisher for specific Mercury topic. The instances should be obtained via {@link TopicPublishersFactory#getPublisherForTopic(String)}.
 * Additionally, Agora core provides an ability to inject the publishers with {@code  @Inject @Named("YourTopicName") TopicPublisher topicPublisher;}.
 * <p>
 * The publisher is thread-safe, so one should use the same instance to construct and publish the messages from different threads simultaneously.
 * <p>
 * It is the main public API point (facade) at publisher side.
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

    /**
     * Construct a message with pre-serialized payload of given contentType
     */
    public MessageToPublish messageWithSerializedPayload(String serializedPayload, String contentType) {
        return new MessageToPublish(serializedPayload, contentType);
    }

    /**
     * Construct a message with plain text payload
     * Shorthand for {@code messageWithSerializedPayload(value, "text/plain")}.
     */
    public MessageToPublish messageWithTextPayload(String value) {
        return messageWithSerializedPayload(value, MercuryMessage.CONTENT_TYPE_PLAIN);
    }

    /**
     * Construct a message with object payload to be automatically serialized to the given contentType.
     * A proper serializer should be registered in {@link MercurySerializers}. Default thrift and json serializers are registered automatically.
     * @throws IllegalStateException if a proper serializer could not be found.
     */
    public <T> MessageToPublish messageWithObjectPayload(T value, String contentType) {
        return messageWithSerializedPayload(MercurySerializers.serialize(value, contentType), contentType);
    }

    /**
     * Construct a message with Thrift-based payload to be automatically serialized to "application/x-thrift+json" contentType.
     * Default thrift serializer for TBase is registered in {@link MercurySerializers} automatically by Agora core.
     * User can register a custom thrift serializer for T class to override the default one.
     * Shorthand for {@code messageWithObjectPayload(value, "application/x-thrift+json")}.
     */
    public <T> MessageToPublish messageWithThriftPayload(T value) {
        return messageWithObjectPayload(value, MercuryMessage.CONTENT_TYPE_THRIFT_JSON);
    }

    /**
     * Construct a message with object payload to be automatically serialized to "application/json" contentType.
     * Default json serializer is registered in {@link MercurySerializers} automatically at startup.
     * User can register a custom json serializer for T class to override the default one.
     * Shorthand for {@code messageWithObjectPayload(value, "application/json")}.
     */
    public <T> MessageToPublish messageWithJsonPayload(T value) {
        return messageWithObjectPayload(value, MercuryMessage.CONTENT_TYPE_JSON);
    }


    /**
     * {@link MercuryMessage} wrapper intended for publication.
     */
    public class MessageToPublish extends MercuryMessage {
        private MessageToPublish(String serializedPayload, String contentType) {
            setSerializedPayload(serializedPayload);
            setContentType(contentType);

            setSenderAppId(senderAppId);
            setTopicName(topicName);
        }

        /**
         * Add custom messageId
         */
        public MessageToPublish withMessageId(String messageId) {
            setMessageId(messageId);
            return this;
        }

        /**
         * Add custom metadata map
         */
        public MessageToPublish addMetadata(Map<String, String> metadata) {
            if(getMetadata() == null) {
                setMetadata(new HashMap<>());
            }
            getMetadata().putAll(metadata);
            return this;
        }

        /**
         * Add custom metadata item
         */
        public MessageToPublish addMetadata(String key, String value) {
            if(getMetadata() == null) {
                setMetadata(new HashMap<>());
            }
            getMetadata().put(key,value);
            return this;
        }

        /**
         * Publish the message to Mercury topic specified by {@link TopicPublisher#topicName}.
         * {@link MercuryMessage#timestamp} is set automatically before publishing.
         * If {@link MercuryMessage#messageId} was not previously set by {@link #withMessageId(String)},
         * then it would be automatically generated by {@link TopicPublishersFactory#messageIdSupplier}
         * @return ID assigned to the published message by SNS.
         */
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