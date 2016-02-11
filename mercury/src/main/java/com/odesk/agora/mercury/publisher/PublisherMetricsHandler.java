package com.odesk.agora.mercury.publisher;

/**
 * Created by Dmitry Solovyov on 02/11/2016.
 * <p>
 * Handler for publisher metrics. Implementation has to be thread-safe.
 */
public interface PublisherMetricsHandler {
    void handlePublicationSuccess(String topicName);

    void handlePublicationFail(String topicName);
}
