package com.odesk.agora.mercury.consumer;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.util.Topics;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import com.odesk.agora.mercury.AgoraMDCData;
import com.odesk.agora.mercury.consumer.config.ConsumerConfiguration;
import com.odesk.agora.mercury.consumer.config.DLQConfiguration;
import com.odesk.agora.mercury.consumer.config.SubscriptionId;
import com.odesk.agora.mercury.consumer.config.TopicSubscriptionConfiguration;
import com.odesk.agora.mercury.publisher.config.PublisherConfiguration;
import com.odesk.agora.mercury.sns.SNSUtils;
import com.odesk.agora.mercury.sqs.SQSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

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

    private final ScheduledExecutorService listenersExecutor;

    /**
     * The default consumptionExecutor would be instantiated as {@code Executors.newCachedThreadPool()}.
     * No metrics handler by default. No agora MDC data processing or cleanup by default.
     * @see #ListenersRunner(ConsumerConfiguration, PublisherConfiguration, AmazonSQSBufferedAsyncClient, AmazonSNSClient, Executor, Consumer, Runnable, ConsumerMetricsHandler) the basic constructor
     */
    public ListenersRunner(ConsumerConfiguration consumerConfig, PublisherConfiguration publisherConfig,
                           AmazonSQSBufferedAsyncClient sqsClient, AmazonSNSClient snsClient) {
        this(consumerConfig, publisherConfig, sqsClient, snsClient, Executors.newCachedThreadPool(), (mdcData) -> {}, ()->{}, null);
    }

    /**
     * @param consumerConfig - consumer configuration
     * @param publisherConfig - publisher configuration (we need it to obtain the prefix for SNS topics)
     * @param sqsClient - instance of {@link AmazonSQSClient}, used to receive and delete the messages
     * @param snsClient - instance of {@link AmazonSNSClient}, used to subscribe a SQS queue to SNS topic
     * @param consumptionExecutor - {@link Executor} for parallel messages consumption
     * @param agoraMDCDataSetter - should propagate agora MDC data to the consumer thread. {@link TopicQueueListener} uses it to apply MDC data passed with a message.
     * @param agoraMDCDataCleaner -should cleanup agora MDC data from the consumer thread. {@link TopicQueueListener} invokes it after message consumption.
     * @param metricsHandler - consumer metrics handler
     */
    public ListenersRunner(ConsumerConfiguration consumerConfig, PublisherConfiguration publisherConfig,
                           AmazonSQSBufferedAsyncClient sqsClient, AmazonSNSClient snsClient,
                           Executor consumptionExecutor, Consumer<AgoraMDCData> agoraMDCDataSetter, Runnable agoraMDCDataCleaner, ConsumerMetricsHandler metricsHandler) {
        checkSubscriptionIds(consumerConfig);

        listenersExecutor = Executors.newScheduledThreadPool(calculateListenersNumber(consumerConfig), new ListenersThreadFactory());

        for (TopicSubscriptionConfiguration subscriptionConfig : consumerConfig.getTopicSubscriptions()) {
            SubscriptionId subId = subscriptionConfig.getSubId();

            String queueUrl = sqsClient.createQueue(SQSUtils.getFullSQSQueueName(consumerConfig, subId)).getQueueUrl();
            sqsClient.setQueueAttributes(queueUrl, subscriptionConfig.asSQSQueueAttributes());

            if (subscriptionConfig.isCreateSNSTopic()) {
                String topicArn = snsClient.createTopic(SNSUtils.getFullSNSTopicName(publisherConfig, subId.getTopicName())).getTopicArn();
                String subscriptionArn = Topics.subscribeQueue(snsClient, sqsClient, topicArn, queueUrl);
                snsClient.setSubscriptionAttributes(subscriptionArn, "RawMessageDelivery", "true");
                logger.info("Mercury topic {} prepared for consuming. TopicArn={}, queueUrl={}, subscriptionArn={}", subId.getTopicName(), topicArn, queueUrl, subscriptionArn);
            } else {
                logger.info("SQS queue {} prepared for consuming. QueueUrl={}", subId, queueUrl);
            }

            listenersExecutor.scheduleWithFixedDelay(
                    new TopicQueueListener(subId, queueUrl, false, sqsClient, consumptionExecutor, agoraMDCDataSetter, agoraMDCDataCleaner, metricsHandler),
                    subscriptionConfig.getPollingIntervalMs(), subscriptionConfig.getPollingIntervalMs(), TimeUnit.MILLISECONDS);

            DLQConfiguration dlqConfig = subscriptionConfig.getDLQConfig();
            if(dlqConfig.isEnabled()) {
                String dlqUrl = sqsClient.createQueue(SQSUtils.getFullSQSQueueNameForDLQ(consumerConfig, subId)).getQueueUrl();
                sqsClient.setQueueAttributes(dlqUrl, dlqConfig.asSQSQueueAttributes());

                String dlqArn = sqsClient.getQueueAttributes(dlqUrl, Arrays.asList("QueueArn")).getAttributes().get("QueueArn");
                sqsClient.setQueueAttributes(queueUrl, Collections.singletonMap("RedrivePolicy",
                        "{\"maxReceiveCount\":\"" + subscriptionConfig.getMaxReceiveCount() + "\", \"deadLetterTargetArn\":\"" + dlqArn + "\"}"));

                logger.info("DLQ {} configured for subscription {}.", dlqUrl, subId);

                listenersExecutor.scheduleWithFixedDelay(
                        new TopicQueueListener(subId, dlqUrl, true, sqsClient, consumptionExecutor, agoraMDCDataSetter, agoraMDCDataCleaner, metricsHandler),
                        dlqConfig.getPollingIntervalMs(), dlqConfig.getPollingIntervalMs(), TimeUnit.MILLISECONDS);
            }
        }
    }

    private void checkSubscriptionIds(ConsumerConfiguration consumerConfig) {
        HashSet<SubscriptionId> ids = new HashSet<>();
        for (TopicSubscriptionConfiguration subscriptionConfig : consumerConfig.getTopicSubscriptions()) {
            if(! ids.add(subscriptionConfig.getSubId())) {
                throw new IllegalStateException("Duplicate subscriptionId: " + subscriptionConfig.getSubId());
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
