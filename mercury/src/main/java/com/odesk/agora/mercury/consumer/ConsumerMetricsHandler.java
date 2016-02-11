package com.odesk.agora.mercury.consumer;

/**
 * Created by Dmitry Solovyov on 02/11/2016.
 * <p>
 * Handler for consumer metrics. Implementation has to be thread-safe.
 */
public interface ConsumerMetricsHandler {
    void handlePollingSuccess(String topicName, int receivedAmount);

    void handlePollingFail(String topicName);

    void handleEmptyPollingResult(String topicName);

    void handleFullPollingResult(String topicName);

    void handleConsumptionSuccess(String topicName);

    void handleConsumptionFail(String topicName);

    void handleDeliveryLatency(String topicName, long latencyMillis);
}
