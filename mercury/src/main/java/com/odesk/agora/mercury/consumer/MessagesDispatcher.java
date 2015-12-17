package com.odesk.agora.mercury.consumer;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.util.Topics;
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import com.odesk.agora.mercury.MercuryMessage;
import com.odesk.agora.mercury.publisher.PublisherConfiguration;
import com.odesk.agora.mercury.publisher.TopicPublishersFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Created by Dmitry Solovyov on 11/27/2015.
 */
public class MessagesDispatcher {
    private static final Logger logger = LoggerFactory.getLogger(MessagesDispatcher.class);

    public static final String QUEUE_NAME_DELIMITER = "-";
    public static final String DLQ_NAME_POSTFIX = "DLQ";

    private ScheduledExecutorService listenersExecutor;

    private final ConcurrentHashMap<String, Consumer<MercuryMessage>> consumers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Consumer<MercuryMessage>> dlqConsumers = new ConcurrentHashMap<>();

    public MessagesDispatcher(ConsumerConfiguration consumerConfig, PublisherConfiguration publisherConfig, AmazonSQSBufferedAsyncClient sqsClient, AmazonSNSClient snsClient) {
        listenersExecutor = Executors.newScheduledThreadPool(consumerConfig.getThreadsCorePoolSize());

        for (TopicSubscriptionConfiguration subscriptionConfig : consumerConfig.getTopicSubscriptions()) {
            String topicName = publisherConfig.getTopicNamesPrefix() + TopicPublishersFactory.TOPIC_NAME_DELIMITER + subscriptionConfig.getTopicName();
            String queueName = consumerConfig.getQueueNamesPrefix() + QUEUE_NAME_DELIMITER + subscriptionConfig.getTopicName();

            String topicArn = snsClient.createTopic(topicName).getTopicArn();

            String queueUrl = sqsClient.createQueue(queueName).getQueueUrl();
            sqsClient.setQueueAttributes(queueUrl, Collections.singletonMap("ReceiveMessageWaitTimeSeconds", "20")); //enable long polling

            String subscriptionArn = Topics.subscribeQueue(snsClient, sqsClient, topicArn, queueUrl);

            logger.info("SNS topic {} prepared for consuming. TopicArn={}, queueUrl={}, subscriptionArn={}", subscriptionConfig.getTopicName(), topicArn, queueUrl, subscriptionArn);

            listenersExecutor.scheduleWithFixedDelay(new TopicQueueListener(subscriptionConfig, sqsClient, queueUrl, this),
                    subscriptionConfig.getPollingIntervalMs(), subscriptionConfig.getPollingIntervalMs(), TimeUnit.MILLISECONDS);

            if(subscriptionConfig.getDLQConfig().isEnabled()) {
                String dlqName = queueName + QUEUE_NAME_DELIMITER + DLQ_NAME_POSTFIX;

                String dlqUrl = sqsClient.createQueue(dlqName).getQueueUrl();
                sqsClient.setQueueAttributes(dlqUrl, Collections.singletonMap("ReceiveMessageWaitTimeSeconds", "20")); //enable long polling for DLQ

                String dlqArn = sqsClient.getQueueAttributes(dlqUrl, Arrays.asList("QueueArn")).getAttributes().get("QueueArn");
                sqsClient.setQueueAttributes(queueUrl, Collections.singletonMap("RedrivePolicy",
                        "{\"maxReceiveCount\":\"" + subscriptionConfig.getMaxReceiveCount() + "\", \"deadLetterTargetArn\":\"" + dlqArn + "\"}"));

                logger.info("DLQ {} configured for topic {}.", dlqUrl, subscriptionConfig.getTopicName());

                listenersExecutor.scheduleWithFixedDelay(new DLQListener(subscriptionConfig, sqsClient, queueUrl, this),
                        subscriptionConfig.getDLQConfig().getPollingIntervalMs(), subscriptionConfig.getDLQConfig().getPollingIntervalMs(), TimeUnit.MILLISECONDS);
            }
        }
    }

    public void setTopicConsumer(String topicName, Consumer<MercuryMessage> consumer) {
        consumers.put(topicName, consumer);
        logger.info("Registered a consumer for topic {}: {}", topicName, consumer);
    }

    public void removeTopicConsumer(String topicName) {
        consumers.remove(topicName);
        logger.info("Consumer for topic {} unregistered", topicName);
    }

    public void setTopicDlqConsumer(String topicName, Consumer<MercuryMessage> consumer) {
        dlqConsumers.put(topicName, consumer);
        logger.info("Registered a DLQ consumer for topic {}: {}", topicName, consumer);
    }

    public void removeTopicDlqConsumer(String topicName) {
        dlqConsumers.remove(topicName);
        logger.info("DLQ consumer for topic {} unregistered", topicName);
    }

    public boolean hasConsumerForTopic(String topicName) {
        return consumers.containsKey(topicName);
    }

    public boolean hasDlqConsumerForTopic(String topicName) {
        return dlqConsumers.containsKey(topicName);
    }

    public void dispatchMessage(MercuryMessage message) {
        Consumer<MercuryMessage> consumer = consumers.get(message.getTopicName());
        if(consumer == null) {
            throw new IllegalStateException("No consumer found for topic " + message.getTopicName());
        } else {
            consumer.accept(message);
        }
    }

    public void dispatchDlqMessage(MercuryMessage message) {
        Consumer<MercuryMessage> dlqConsumer = dlqConsumers.get(message.getTopicName());
        if(dlqConsumer == null) {
            throw new IllegalStateException("No DLQ consumer found for topic " + message.getTopicName());
        } else {
            dlqConsumer.accept(message);
        }
    }
}
