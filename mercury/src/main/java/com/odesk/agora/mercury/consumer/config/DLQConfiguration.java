package com.odesk.agora.mercury.consumer.config;

import javax.validation.constraints.NotNull;

/**
 * Created by Dmitry Solovyov on 12/14/2015.
 * <p>
 * Dead letter queue configuration for some topic subscription.
 * @see <a href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/SQSDeadLetterQueue.html">SQS documentation for dead letter queues</a>
 */
public class DLQConfiguration extends QueueConfiguration {
    private static final long DEFAULT_POLLING_INTERVAL_MS = 5000;
    private static final int DEFAULT_VISIBILITY_TIMEOUT_SEC = 60;
    private static final int DEFAULT_POLLING_WAIT_TIME_SEC = 20; //Enable long polling by default. Set to zero if you want short polling instead.
    private static final int DEFAULT_DELAY_SEC = 0;

    public DLQConfiguration() {
        super(DEFAULT_POLLING_INTERVAL_MS, DEFAULT_VISIBILITY_TIMEOUT_SEC, DEFAULT_POLLING_WAIT_TIME_SEC, DEFAULT_DELAY_SEC);
    }

    /**
     * Is DLQ enabled or disabled for the topic subscription.
     * Default is true.
     */
    @NotNull
    private boolean enabled = true;

    public boolean isEnabled() {
        return enabled;
    }

}
