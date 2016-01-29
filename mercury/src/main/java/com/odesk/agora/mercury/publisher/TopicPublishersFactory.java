package com.odesk.agora.mercury.publisher;

import com.amazonaws.services.sns.AmazonSNSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Created by Dmitry Solovyov on 12/08/2015.
 *
 * The factory for all topic publishers. Designed to be a singleton. Agora core instantiates it with Mercury Guice module and makes it injectable.
 */
public class TopicPublishersFactory {
    private static final Logger logger = LoggerFactory.getLogger(TopicPublishersFactory.class);

    public static final String TOPIC_NAME_DELIMITER = "-";

    private final PublisherConfiguration publisherConfig;
    private final AmazonSNSClient snsClient;
    private final String senderAppId;
    private Supplier<String> messageIdSupplier;

    private ConcurrentHashMap<String, TopicPublisher> topicPublishers = new ConcurrentHashMap<>();

    /**
     * @see TopicPublishersFactory#TopicPublishersFactory(PublisherConfiguration, AmazonSNSClient, String, Supplier)
     * The default messageIdSupplier would just generate a random UUID string.
     */
    public TopicPublishersFactory(PublisherConfiguration publisherConfig, AmazonSNSClient snsClient, String senderAppId) {
        this(publisherConfig, snsClient, senderAppId, () -> UUID.randomUUID().toString());
    }

    /**
     * @param publisherConfig - publisher configuration
     * @param snsClient - instance of {@link AmazonSNSClient} to be used for publishing
     * @param senderAppId - the ID of sending (publishing) application. Agora core sets it to the service name.
     * @param messageIdSupplier - the supplier for message IDs. {@link TopicPublisher.MessageToPublish#publish()} uses it to generate an ID before publication if it's not set explicitly,
     */
    public TopicPublishersFactory(PublisherConfiguration publisherConfig, AmazonSNSClient snsClient, String senderAppId, Supplier<String> messageIdSupplier) {
        this.publisherConfig = publisherConfig;
        this.snsClient = snsClient;
        this.senderAppId = senderAppId;
        this.messageIdSupplier = messageIdSupplier;
    }

    /**
     * Obtain a {@link TopicPublisher} for Mercury topic specified by topicName. The publishers are thread-safe, so this method always returns the same instance per topicName.
     */
    public TopicPublisher getPublisherForTopic(String topicName) {
        return topicPublishers.computeIfAbsent(publisherConfig.getTopicNamesPrefix() + TOPIC_NAME_DELIMITER + topicName, prefixedName -> {
            String topicArn = snsClient.createTopic(prefixedName).getTopicArn();
            logger.info("SNS topic {} prepared for publishing. TopicArn is {}", topicName, topicArn);
            return new TopicPublisher(snsClient, topicName, topicArn, senderAppId, messageIdSupplier);
        });
    }
}
