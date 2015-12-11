package com.odesk.agora.mercury.publisher;

import com.amazonaws.services.sns.AmazonSNSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Dmitry Solovyov on 12/08/2015.
 */
public class TopicPublishersFactory {
    private static final Logger logger = LoggerFactory.getLogger(TopicPublishersFactory.class);

    public static final String TOPIC_NAME_DELIMITER = "-";

    private final PublisherConfiguration publisherConfig;
    private final AmazonSNSClient snsClient;

    private ConcurrentHashMap<String, TopicPublisher> topicPublishers = new ConcurrentHashMap<>();

    public TopicPublishersFactory(PublisherConfiguration publisherConfig, AmazonSNSClient snsClient) {
        this.publisherConfig = publisherConfig;
        this.snsClient = snsClient;
    }

    public TopicPublisher getPublisherForTopic(String topicName) {
        return topicPublishers.computeIfAbsent(publisherConfig.getTopicNamesPrefix() + TOPIC_NAME_DELIMITER + topicName, prefixedName -> {
            String topicArn = snsClient.createTopic(prefixedName).getTopicArn();
            logger.info("SNS topic {} prepared for publishing. TopicArn is {}", topicName, topicArn);
            return new TopicPublisher(snsClient, topicName, topicArn);
        });
    }
}
