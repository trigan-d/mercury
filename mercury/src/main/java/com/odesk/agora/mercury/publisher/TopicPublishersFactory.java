package com.odesk.agora.mercury.publisher;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.odesk.agora.mercury.AgoraMDCData;
import com.odesk.agora.mercury.publisher.config.PublisherConfiguration;
import com.odesk.agora.mercury.sns.SNSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Created by Dmitry Solovyov on 12/08/2015.
 * <p>
 * The factory for all topic publishers. Designed to be a singleton. Agora core instantiates it with Mercury Guice module and makes it injectable.
 */
public class TopicPublishersFactory {
    private static final Logger logger = LoggerFactory.getLogger(TopicPublishersFactory.class);

    private final PublisherConfiguration publisherConfig;
    private final AmazonSNSClient snsClient;
    private final String senderAppId;
    private final Supplier<String> messageIdSupplier;
    private final Supplier<AgoraMDCData> agoraMDCDataGetter;
    private final PublisherMetricsHandler metricsHandler;

    private ConcurrentHashMap<String, TopicPublisher> topicPublishers = new ConcurrentHashMap<>();

    /**
     * The default messageIdSupplier would just generate a random UUID string.
     * No metrics handler by default. Null agora MDC data supplied by default.
     * @see #TopicPublishersFactory(PublisherConfiguration, AmazonSNSClient, String, Supplier, Supplier, PublisherMetricsHandler) the basic constructor
     */
    public TopicPublishersFactory(PublisherConfiguration publisherConfig, AmazonSNSClient snsClient, String senderAppId) {
        this(publisherConfig, snsClient, senderAppId, () -> UUID.randomUUID().toString(), () -> null, null);
    }

    /**
     * @param publisherConfig - publisher configuration
     * @param snsClient - instance of {@link AmazonSNSClient} to be used for publishing
     * @param senderAppId - the ID of sending (publishing) application. Agora core sets it to the service name.
     * @param messageIdSupplier - the supplier for message IDs. {@link TopicPublisher.MessageToPublish#publish()} uses it to generate an ID before publication if it's not set explicitly.
     * @param agoraMDCDataGetter - should obtain agora MDC data from the publisher thread. {@link TopicPublisher} uses it to attach MDC data to every message created.
     * @param metricsHandler - publisher metrics handler
     */
    public TopicPublishersFactory(PublisherConfiguration publisherConfig, AmazonSNSClient snsClient, String senderAppId,
                                  Supplier<String> messageIdSupplier, Supplier<AgoraMDCData> agoraMDCDataGetter, PublisherMetricsHandler metricsHandler) {
        this.publisherConfig = publisherConfig;
        this.snsClient = snsClient;
        this.senderAppId = senderAppId;
        this.messageIdSupplier = messageIdSupplier;
        this.agoraMDCDataGetter = agoraMDCDataGetter;
        this.metricsHandler = metricsHandler;
    }

    /**
     * Obtain a {@link TopicPublisher} for Mercury topic given by topicName. The publishers are thread-safe, so this method always returns the same instance per topicName.
     * Automatically creates SNS topic if it doesn't exist yet.
     */
    public TopicPublisher getPublisherForTopic(String topicName) {
        return topicPublishers.computeIfAbsent(SNSUtils.getFullSNSTopicName(publisherConfig, topicName), fullSNSTopicName -> {
            String topicArn = snsClient.createTopic(fullSNSTopicName).getTopicArn();
            logger.info("SNS topic {} prepared for publishing. TopicArn is {}", topicName, topicArn);
            return new TopicPublisher(topicName, topicArn, snsClient, senderAppId, messageIdSupplier, agoraMDCDataGetter, metricsHandler);
        });
    }
}
