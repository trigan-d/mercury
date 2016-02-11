package com.odesk.agora.mercury.consumer;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.util.Topics;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import com.odesk.agora.mercury.consumer.config.ConsumerConfiguration;
import com.odesk.agora.mercury.consumer.config.DLQConfiguration;
import com.odesk.agora.mercury.consumer.config.TopicSubscriptionConfiguration;
import com.odesk.agora.mercury.publisher.PublisherConfiguration;
import com.odesk.agora.mercury.publisher.TopicPublishersFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Dmitry Solovyov on 11/27/2015.
 * <p>
 * The factory and scheduler for all topic listeners. Designed to be a singleton. Agora core instantiates it with Mercury Guice module and makes it injectable.
 * Automatically creates SQS queues and subscribes them to SNS topics if they don't exist yet.
 * <p>
 * Not intended to be used by end-users.
 */
public class ListenersRunner {
    private static final Logger logger = LoggerFactory.getLogger(ListenersRunner.class);

    public static final String QUEUE_NAME_DELIMITER = "-";
    public static final String DLQ_NAME_POSTFIX = "DLQ";

    private final ScheduledExecutorService listenersExecutor;

    /**
     * The default consumptionExecutor would be instantiated as {@code Executors.newCachedThreadPool()}.
     * No metrics handler by default.
     * @see #ListenersRunner(ConsumerConfiguration, PublisherConfiguration, AmazonSQSBufferedAsyncClient, AmazonSNSClient, Executor, ConsumerMetricsHandler) the basic constructor
     */
    public ListenersRunner(ConsumerConfiguration consumerConfig, PublisherConfiguration publisherConfig,
                           AmazonSQSBufferedAsyncClient sqsClient, AmazonSNSClient snsClient) {
        this(consumerConfig, publisherConfig, sqsClient, snsClient, Executors.newCachedThreadPool(), null);
    }

    /**
     * @param consumerConfig - consumer configuration
     * @param publisherConfig - publisher configuration (we need it to obtain the prefix for SNS topics)
     * @param sqsClient - instance of {@link AmazonSQSClient}, used to receive and delete the messages
     * @param snsClient - instance of {@link AmazonSNSClient}, used to subscribe a SQS queue to SNS topic
     * @param consumptionExecutor - {@link Executor} for parallel messages consumption
     * @param metricsHandler - consumer metrics handler
     */
    public ListenersRunner(ConsumerConfiguration consumerConfig, PublisherConfiguration publisherConfig,
                           AmazonSQSBufferedAsyncClient sqsClient, AmazonSNSClient snsClient,
                           Executor consumptionExecutor, ConsumerMetricsHandler metricsHandler) {
        listenersExecutor = Executors.newScheduledThreadPool(calculateListenersNumber(consumerConfig), new ListenersThreadFactory());

        for (TopicSubscriptionConfiguration subscriptionConfig : consumerConfig.getTopicSubscriptions()) {
            String topicName = publisherConfig.getTopicNamesPrefix() + TopicPublishersFactory.TOPIC_NAME_DELIMITER + subscriptionConfig.getTopicName();
            String queueName = consumerConfig.getQueueNamesPrefix() + QUEUE_NAME_DELIMITER + subscriptionConfig.getTopicName();

            String queueUrl = sqsClient.createQueue(queueName).getQueueUrl();
            sqsClient.setQueueAttributes(queueUrl, subscriptionConfig.asSQSQueueAttributes());

            if (subscriptionConfig.isCreateSNSTopic()) {
                String topicArn = snsClient.createTopic(topicName).getTopicArn();
                String subscriptionArn = Topics.subscribeQueue(snsClient, sqsClient, topicArn, queueUrl);
                snsClient.setSubscriptionAttributes(subscriptionArn, "RawMessageDelivery", "true");
                logger.info("Mercury topic {} prepared for consuming. TopicArn={}, queueUrl={}, subscriptionArn={}", subscriptionConfig.getTopicName(), topicArn, queueUrl, subscriptionArn);
            } else {
                logger.info("SQS queue {} prepared for consuming. QueueUrl={}", subscriptionConfig.getTopicName(), queueUrl);
            }

            listenersExecutor.scheduleWithFixedDelay(new TopicQueueListener(subscriptionConfig.getTopicName(), queueUrl,
                                                                            false, sqsClient, consumptionExecutor, metricsHandler),
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

                listenersExecutor.scheduleWithFixedDelay(new TopicQueueListener(subscriptionConfig.getTopicName(), dlqUrl,
                                                                                true, sqsClient, consumptionExecutor, metricsHandler),
                        dlqConfig.getPollingIntervalMs(), dlqConfig.getPollingIntervalMs(), TimeUnit.MILLISECONDS);
            }
        }
    }

    public void shutdown() {
        listenersExecutor.shutdown();
    }

    private int calculateListenersNumber(ConsumerConfiguration consumerConfig) {
        int listeners = 0;
        for (TopicSubscriptionConfiguration subscriptionConfig : consumerConfig.getTopicSubscriptions()) {
            listeners += subscriptionConfig.getDLQConfig().isEnabled() ? 2 : 1;
        }
        return listeners;
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
