package com.odesk.agora.mercury.sqs;

import com.odesk.agora.mercury.consumer.TopicSubscriptionConfiguration;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.List;

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
}
