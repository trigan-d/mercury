package com.odesk.agora.mercury.consumer;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * Created by Dmitry Solovyov on 11/25/2015.
 */
public class TopicSubscriptionConfiguration {
    @NotNull
    private String topicName;

    @NotNull
    @Min(100)
    private long pollingIntervalMs;

    @NotNull
    @Min(1)
    @Max(100)
    private int maxReceiveCount = 5;

    @Valid
    private DLQConfiguration dlq;

    public String getTopicName() {
        return topicName;
    }

    public long getPollingIntervalMs() {
        return pollingIntervalMs;
    }

    public int getMaxReceiveCount() {
        return maxReceiveCount;
    }

    public DLQConfiguration getDLQConfig() {
        return dlq;
    }
}
