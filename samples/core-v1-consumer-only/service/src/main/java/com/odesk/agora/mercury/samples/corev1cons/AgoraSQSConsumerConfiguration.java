package com.odesk.agora.mercury.samples.corev1cons;

import com.google.common.collect.ImmutableList;
import com.odesk.agora.configuration.AwsServiceConfiguration;
import com.odesk.agora.mercury.consumer.SQSConsumerConfiguration;
import com.odesk.agora.mercury.consumer.TopicSubscriptionConfiguration;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Dmitry Solovyov on 11/30/2015.
 */
public class AgoraSQSConsumerConfiguration extends AwsServiceConfiguration implements SQSConsumerConfiguration {
    private int consumerThreadsCorePoolSize;

    private String snsEndpoint;

    private String queueNamesPrefix;

    private ImmutableList<TopicSubscriptionConfiguration> subscriptions;

    private TopicSubscriptionConfiguration subscription1;
    private TopicSubscriptionConfiguration subscription2;
    private TopicSubscriptionConfiguration subscription3;
    private TopicSubscriptionConfiguration subscription4;
    //etc...

    @Override
    public int getConsumerThreadsCorePoolSize() {
        return consumerThreadsCorePoolSize;
    }

    @Override
    public String getSNSEndpoint() {
        return snsEndpoint;
    }

    @Override
    public String getQueueNamesPrefix() {
        return queueNamesPrefix;
    }

    @Override
    public List<TopicSubscriptionConfiguration> getTopicSubscriptions() {
        if (subscriptions == null) {
            subscriptions = ImmutableList.copyOf(Arrays.asList(
                    subscription1, subscription2, subscription3, subscription4 //etc...
            ).stream().filter(sub -> (sub != null && sub.getTopicName() != null)).collect(Collectors.toList()));
        }
        return subscriptions;
    }
}
