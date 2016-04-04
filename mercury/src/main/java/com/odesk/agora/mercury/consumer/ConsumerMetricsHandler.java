package com.odesk.agora.mercury.consumer;

/**
 * Created by Dmitry Solovyov on 02/11/2016.
 * <p>
 * Handler for consumer metrics. Implementation has to be thread-safe.
 */
public interface ConsumerMetricsHandler {
    void handlePollingFail(String subscriptionId);

    void handlePollingSuccess(String subscriptionId, int receivedAmount);

    void handleEmptyPollingResult(String subscriptionId);

    void handleFullPollingResult(String subscriptionId);

    void handleDeliveryLatency(String subscriptionId, long latencyMillis);

    void handleConsumptionFail(String subscriptionId);

    void handleConsumptionDuration(String subscriptionId, long durationMillis);
}
