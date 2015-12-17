package com.odesk.agora.mercury.consumer;

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

    public String getTopicName() {
        return topicName;
    }

    public long getPollingIntervalMs() {
        return pollingIntervalMs;
    }
}
