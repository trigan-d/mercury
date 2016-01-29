package com.odesk.agora.mercury.consumer;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.HashMap;

/**
 * Created by Dmitry Solovyov on 11/25/2015.
 * <p>
 * Configuration for SQS queue polling and processing
 */
public abstract class QueueConfiguration {
    /**
     * Interval for queue polling, in milliseconds. Mercury polls new messages from the queue periodically with the specified interval.
     * Decrease the interval if the query has heavy traffic and overflows faster than Mercury consumes the messages.
     * Increase the interval if the query has small traffic and Mercury receives a small or zero amount of messages on each poll. Thus we could reduce our costs.
     * Default is 5000 for subscription queue and 10000 for dead letter queue
     */
    @NotNull
    @Min(100)
    private long pollingIntervalMs;

    /**
     * Visibility timeout for SQS messages, in seconds. After a message gets polled, it becomes invisible for the consequent polls for this amount of time.
     * So, it's a time allotted for message processing. Increase it if the processing of each message takes a lot of time.
     * Default is 30 for subscription queue and 60 for dead letter queue
     * @see <a href="http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/AboutVT.html">SQS documentation for visibility timeout</a>
     */
    @NotNull
    @Min(0)
    @Max(43200)
    private int visibilityTimeoutSec;

    /**
     * Set to 0 for SQS short polling. Each SQS poll would finish immediately even there are no new messages in a queue.
     * Set from 1 to 20 for SQS long polling. Each SQS poll would wait for the new messages up to the specified amount of time in seconds. If the queue is not empty, the poll finishes immediately as well.
     * Default is 20 (long polling). It suits most cases.
     * @see <a href="http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html">SQS documentation for long polling</a>
     */
    @NotNull
    @Min(0)
    @Max(20)
    private int pollingWaitTimeSec;

    /**
     * All new messages in the queue would be available for polling only after the specified amount of time in seconds.
     * Default is 0 - messages are available immediately.
     * @see <a href="http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-delay-queues.html">SQS documentation for delay queues</a>
     */
    @NotNull
    @Min(0)
    @Max(900)
    private int delaySec;

    protected QueueConfiguration(long pollingIntervalMs, int visibilityTimeoutSec, int pollingWaitTimeSec, int delaySec) {
        this.pollingIntervalMs = pollingIntervalMs;
        this.visibilityTimeoutSec = visibilityTimeoutSec;
        this.pollingWaitTimeSec = pollingWaitTimeSec;
        this.delaySec = delaySec;
    }

    /**
     * @return a map of SQS queue attributes
     */
    public HashMap<String, String> asSQSQueueAttributes() {
        HashMap<String, String> map = new HashMap<>();

        map.put("VisibilityTimeout", String.valueOf(visibilityTimeoutSec));
        map.put("ReceiveMessageWaitTimeSeconds", String.valueOf(pollingWaitTimeSec));
        map.put("DelaySeconds", String.valueOf(delaySec));

        return map;
    }

    public long getPollingIntervalMs() {
        return pollingIntervalMs;
    }

    public int getVisibilityTimeoutSec() {
        return visibilityTimeoutSec;
    }

    public int getPollingWaitTimeSec() {
        return pollingWaitTimeSec;
    }

    public int getDelaySec() {
        return delaySec;
    }
}
