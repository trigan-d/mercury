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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Created by Dmitry Solovyov on 11/27/2015.
 */
public class MessagesDispatcher {
    private static final Logger logger = LoggerFactory.getLogger(MessagesDispatcher.class);

    public static final String QUEUE_NAME_DELIMITER = "-";
    public static final String DLQ_NAME_POSTFIX = "DLQ";

    private final ConsumerConfiguration consumerConfig;
    private final PublisherConfiguration publisherConfig;
    private final AmazonSQSBufferedAsyncClient sqsClient;
    private final AmazonSNSClient snsClient;

    private ScheduledExecutorService listenersExecutor;
    private Executor consumptionExecutor;

    private final ConcurrentHashMap<String, Consumer<MercuryMessage>> consumers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Consumer<MercuryMessage>> dlqConsumers = new ConcurrentHashMap<>();

    public MessagesDispatcher(ConsumerConfiguration consumerConfig, PublisherConfiguration publisherConfig, AmazonSQSBufferedAsyncClient sqsClient, AmazonSNSClient snsClient) {
        this(consumerConfig, publisherConfig, sqsClient, snsClient, Executors.newCachedThreadPool());
    }

    public MessagesDispatcher(ConsumerConfiguration consumerConfig, PublisherConfiguration publisherConfig, AmazonSQSBufferedAsyncClient sqsClient,
                              AmazonSNSClient snsClient, Executor consumptionExecutor) {
        this.consumerConfig = consumerConfig;
        this.publisherConfig = publisherConfig;
        this.sqsClient = sqsClient;
        this.snsClient = snsClient;
        this.consumptionExecutor = consumptionExecutor;

        listenersExecutor = Executors.newScheduledThreadPool(calculateListenersNumber(), new ListenersThreadFactory());

        for (TopicSubscriptionConfiguration subscriptionConfig : consumerConfig.getTopicSubscriptions()) {
            String topicName = publisherConfig.getTopicNamesPrefix() + TopicPublishersFactory.TOPIC_NAME_DELIMITER + subscriptionConfig.getTopicName();
            String queueName = consumerConfig.getQueueNamesPrefix() + QUEUE_NAME_DELIMITER + subscriptionConfig.getTopicName();

            String topicArn = snsClient.createTopic(topicName).getTopicArn();

            String queueUrl = sqsClient.createQueue(queueName).getQueueUrl();
            sqsClient.setQueueAttributes(queueUrl, subscriptionConfig.asSQSQueueAttributes());

            String subscriptionArn = Topics.subscribeQueue(snsClient, sqsClient, topicArn, queueUrl);

            logger.info("SNS topic {} prepared for consuming. TopicArn={}, queueUrl={}, subscriptionArn={}", subscriptionConfig.getTopicName(), topicArn, queueUrl, subscriptionArn);

            listenersExecutor.scheduleWithFixedDelay(new TopicQueueListener(subscriptionConfig, sqsClient, queueUrl, this),
                    subscriptionConfig.getPollingIntervalMs(), subscriptionConfig.getPollingIntervalMs(), TimeUnit.MILLISECONDS);

            DLQConfiguration dlqConfig = subscriptionConfig.getDLQConfig();
            if(dlqConfig.isEnabled()) {
                String dlqName = queueName + QUEUE_NAME_DELIMITER + DLQ_NAME_POSTFIX;

                String dlqUrl = sqsClient.createQueue(dlqName).getQueueUrl();
                sqsClient.setQueueAttributes(dlqUrl, dlqConfig.asSQSQueueAttributes());

                String dlqArn = sqsClient.getQueueAttributes(dlqUrl, Arrays.asList("QueueArn")).getAttributes().get("QueueArn");
                sqsClient.setQueueAttributes(queueUrl, Collections.singletonMap("RedrivePolicy",
                        "{\"maxReceiveCount\":\"" + subscriptionConfig.getMaxReceiveCount() + "\", \"deadLetterTargetArn\":\"" + dlqArn + "\"}"));

                logger.info("DLQ {} configured for topic {}.", dlqUrl, subscriptionConfig.getTopicName());

                listenersExecutor.scheduleWithFixedDelay(new DLQListener(subscriptionConfig, sqsClient, dlqUrl, this),
                        dlqConfig.getPollingIntervalMs(), dlqConfig.getPollingIntervalMs(), TimeUnit.MILLISECONDS);
            }
        }
    }

    private int calculateListenersNumber() {
        int listeners = 0;
        for (TopicSubscriptionConfiguration subscriptionConfig : consumerConfig.getTopicSubscriptions()) {
            listeners += subscriptionConfig.getDLQConfig().isEnabled() ? 2 : 1;
        }
        return listeners;
    }

    public void consumeAsync(ConsumptionRunnable consumption) {
        consumptionExecutor.execute(consumption);
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


    private static class ListenersThreadFactory implements ThreadFactory {
        static AtomicInteger threadCount = new AtomicInteger( 0 );
        public Thread newThread( Runnable r) {
            int threadNumber = threadCount.addAndGet(1);
            Thread thread = new Thread( r );
            thread.setDaemon(true);
            thread.setName("MercuryTopicListenerThread-" + threadNumber );
            return thread;
        }
    }
}
