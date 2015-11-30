package com.odesk.agora.mercury.samples.corev2pubcons;

import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.odesk.agora.configuration.AwsServiceConfiguration;
import com.odesk.agora.mercury.consumer.SQSConsumerConfiguration;
import com.odesk.agora.mercury.consumer.TopicSubscriptionConfiguration;

import javax.validation.Valid;
import java.util.Arrays;
import java.util.List;

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
            subscriptions = ImmutableList.copyOf(Collections2.filter(Arrays.asList(
                    subscription1, subscription2, subscription3, subscription4 //etc...
            ), Predicates.notNull()));
        }
        return subscriptions;
    }
}
