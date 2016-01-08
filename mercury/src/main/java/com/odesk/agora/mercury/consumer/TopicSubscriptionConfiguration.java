package com.odesk.agora.mercury.consumer;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * Created by Dmitry Solovyov on 11/25/2015.
 */
public class TopicSubscriptionConfiguration extends QueueConfiguration {
    public static final long DEFAULT_POLLING_INTEVAL_MS = 5000;
    public static final int DEFAULT_VISIBILITY_TIMEOUT_SEC = 30;
    public static final int DEFAULT_POLLING_WAIT_TIME_SEC = 20; //Enable long polling by default. Set to zero if you want short polling instead.
    public static final int DEFAULT_DELAY_SEC = 0;

    public TopicSubscriptionConfiguration() {
        super(DEFAULT_POLLING_INTEVAL_MS, DEFAULT_VISIBILITY_TIMEOUT_SEC, DEFAULT_POLLING_WAIT_TIME_SEC, DEFAULT_DELAY_SEC);
    }
    @NotNull
    private String topicName;

    @NotNull
    @Min(1)
    @Max(100)
    private int maxReceiveCount = 5;

    @Valid
    private DLQConfiguration dlq;

    public String getTopicName() {
        return topicName;
    }

    public int getMaxReceiveCount() {
        return maxReceiveCount;
    }

    public DLQConfiguration getDLQConfig() {
        return dlq;
    }
}
