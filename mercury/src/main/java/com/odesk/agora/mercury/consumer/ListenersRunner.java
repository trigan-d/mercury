package com.odesk.agora.mercury.consumer;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.util.Topics;
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
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
 */
public class ListenersRunner {
    private static final Logger logger = LoggerFactory.getLogger(ListenersRunner.class);

    public static final String QUEUE_NAME_DELIMITER = "-";
    public static final String DLQ_NAME_POSTFIX = "DLQ";

    private ScheduledExecutorService listenersExecutor;

    public ListenersRunner(ConsumerConfiguration consumerConfig, PublisherConfiguration publisherConfig, AmazonSQSBufferedAsyncClient sqsClient, AmazonSNSClient snsClient) {
        this(consumerConfig, publisherConfig, sqsClient, snsClient, Executors.newCachedThreadPool());
    }

    public ListenersRunner(ConsumerConfiguration consumerConfig, PublisherConfiguration publisherConfig, AmazonSQSBufferedAsyncClient sqsClient,
                           AmazonSNSClient snsClient, Executor consumptionExecutor) {
        listenersExecutor = Executors.newScheduledThreadPool(calculateListenersNumber(consumerConfig), new ListenersThreadFactory());

        for (TopicSubscriptionConfiguration subscriptionConfig : consumerConfig.getTopicSubscriptions()) {
            String topicName = publisherConfig.getTopicNamesPrefix() + TopicPublishersFactory.TOPIC_NAME_DELIMITER + subscriptionConfig.getTopicName();
            String queueName = consumerConfig.getQueueNamesPrefix() + QUEUE_NAME_DELIMITER + subscriptionConfig.getTopicName();

            String topicArn = snsClient.createTopic(topicName).getTopicArn();

            String queueUrl = sqsClient.createQueue(queueName).getQueueUrl();
            sqsClient.setQueueAttributes(queueUrl, subscriptionConfig.asSQSQueueAttributes());

            String subscriptionArn = Topics.subscribeQueue(snsClient, sqsClient, topicArn, queueUrl);
            snsClient.setSubscriptionAttributes(subscriptionArn, "RawMessageDelivery", "true");

            logger.info("SNS topic {} prepared for consuming. TopicArn={}, queueUrl={}, subscriptionArn={}", subscriptionConfig.getTopicName(), topicArn, queueUrl, subscriptionArn);

            listenersExecutor.scheduleWithFixedDelay(new TopicQueueListener(subscriptionConfig.getTopicName(), queueUrl, false, sqsClient, consumptionExecutor),
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

                listenersExecutor.scheduleWithFixedDelay(new TopicQueueListener(subscriptionConfig.getTopicName(), dlqUrl, true, sqsClient, consumptionExecutor),
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
