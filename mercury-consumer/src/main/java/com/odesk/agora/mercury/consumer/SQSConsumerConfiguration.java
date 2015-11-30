package com.odesk.agora.mercury.consumer;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Created by Dmitry Solovyov on 11/23/2015.
 */
public interface SQSConsumerConfiguration {
    boolean isEnabled();

    String getEndpoint();

    int getSocketTimeout();

    int getConnectionTimeout();

    int getMaxConnections();

    int getMaxErrorRetry();

    String getAccessKey();

    String getSecretKey();


    @NotNull
    @Min(1)
    @Max(10)
    int getConsumerThreadsCorePoolSize();

    @NotNull
    String getSNSEndpoint();

    @NotNull
    String getQueueNamesPrefix();

    List<TopicSubscriptionConfiguration> getTopicSubscriptions();
}
