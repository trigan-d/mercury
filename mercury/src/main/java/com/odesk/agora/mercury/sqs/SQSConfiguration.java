package com.odesk.agora.mercury.sqs;

import com.amazonaws.services.sqs.buffered.QueueBufferConfig;

/**
 * Created by Dmitry Solovyov on 11/23/2015.
 */
public interface SQSConfiguration {
    String getEndpoint();

    int getSocketTimeout();

    int getConnectionTimeout();

    int getMaxConnections();

    int getMaxErrorRetry();

    String getAccessKey();

    String getSecretKey();

    QueueBufferConfig getQueueBufferConfig();
}
