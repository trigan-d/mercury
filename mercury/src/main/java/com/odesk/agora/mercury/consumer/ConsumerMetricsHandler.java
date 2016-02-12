package com.odesk.agora.mercury.consumer;

/**
 * Created by Dmitry Solovyov on 02/11/2016.
 * <p>
 * Handler for consumer metrics. Implementation has to be thread-safe.
 */
public interface ConsumerMetricsHandler {
    void handlePollingFail(String topicName);

    void handlePollingSuccess(String topicName, int receivedAmount);

    void handleEmptyPollingResult(String topicName);

    void handleFullPollingResult(String topicName);

    void handleDeliveryLatency(String topicName, long latencyMillis);

    void handleConsumptionFail(String topicName);

    void handleConsumptionDuration(String topicName, long durationMillis);
}
