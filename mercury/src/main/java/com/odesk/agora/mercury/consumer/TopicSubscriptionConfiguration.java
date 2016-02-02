package com.odesk.agora.mercury.consumer;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * Created by Dmitry Solovyov on 11/25/2015.
 * <p>
 * Configuration for SQS queues subscribed to Mercury SNS topics.
 */
public class TopicSubscriptionConfiguration extends QueueConfiguration {
    public static final long DEFAULT_POLLING_INTERVAL_MS = 5000;
    public static final int DEFAULT_VISIBILITY_TIMEOUT_SEC = 30;
    public static final int DEFAULT_POLLING_WAIT_TIME_SEC = 20; //Enable long polling by default. Set to zero if you want short polling instead.
    public static final int DEFAULT_DELAY_SEC = 0;

    public TopicSubscriptionConfiguration() {
        super(DEFAULT_POLLING_INTERVAL_MS, DEFAULT_VISIBILITY_TIMEOUT_SEC, DEFAULT_POLLING_WAIT_TIME_SEC, DEFAULT_DELAY_SEC);
    }

    /**
     * The name of Mercury topic to subscribe.
     */
    @NotNull
    private String topicName;

    /**
     * The max number of attempts to process the message.
     * Each Mercury message gets automatically deleted from the SQS queue after successful processing.
     * If the processing fails then the message stays in the queue and would be polled and processed again.
     * After the specified number of failed attempts the message gets completely removed from the queue, or sent to the DLQ if it is enabled.
     * @see TopicSubscriptionConfiguration#dlq
     */
    @NotNull
    @Min(1)
    @Max(100)
    private int maxReceiveCount = 5;

    /**
     * Dead letter queue configuration for this topic subscription.
     * @see <a href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/SQSDeadLetterQueue.html">SQS documentation for dead letter queues</a>
     */
    @Valid
    private DLQConfiguration dlq;

    /**
     * Should the consumer create the SNS topic and SNS-SQS subscription automatically.
     * If set to false then the consumer creates only the SQS queue.
     * <p>
     * Default is true. It is the standard setup for Mercury communication.
     * <p>
     * You should change it only when you want to consume the messages from some SQS queue that is populated not by Mercury.
     * Also consider using {@link PlainSQSConsumers} in that case to process non-Mercury SQS messages.
     */
    @NotNull
    private boolean createSNSTopic = true;

    public String getTopicName() {
        return topicName;
    }

    public int getMaxReceiveCount() {
        return maxReceiveCount;
    }

    public DLQConfiguration getDLQConfig() {
        return dlq;
    }

    public boolean isCreateSNSTopic() {
        return createSNSTopic;
    }
}
