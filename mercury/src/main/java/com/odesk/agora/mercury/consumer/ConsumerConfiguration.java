package com.odesk.agora.mercury.consumer;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Dmitry Solovyov on 12/07/2015.
 */
public class ConsumerConfiguration {
    @NotNull
    private boolean enabled;

    @NotNull
    @Min(1)
    @Max(10)
    private int threadsCorePoolSize;

    @NotNull
    private String queueNamesPrefix;

    private TopicSubscriptionConfiguration subscription1;
    private TopicSubscriptionConfiguration subscription2;
    private TopicSubscriptionConfiguration subscription3;
    private TopicSubscriptionConfiguration subscription4;
    private TopicSubscriptionConfiguration subscription5;
    private TopicSubscriptionConfiguration subscription6;
    private TopicSubscriptionConfiguration subscription7;
    private TopicSubscriptionConfiguration subscription8;
    private TopicSubscriptionConfiguration subscription9;
    private TopicSubscriptionConfiguration subscription10;
    private TopicSubscriptionConfiguration subscription11;
    private TopicSubscriptionConfiguration subscription12;
    private TopicSubscriptionConfiguration subscription13;
    private TopicSubscriptionConfiguration subscription14;
    private TopicSubscriptionConfiguration subscription15;
    private TopicSubscriptionConfiguration subscription16;
    private TopicSubscriptionConfiguration subscription17;
    private TopicSubscriptionConfiguration subscription18;
    private TopicSubscriptionConfiguration subscription19;
    private TopicSubscriptionConfiguration subscription20;
    //etc...

    public int getThreadsCorePoolSize() {
        return threadsCorePoolSize;
    }

    public String getQueueNamesPrefix() {
        return queueNamesPrefix;
    }

    public List<TopicSubscriptionConfiguration> getTopicSubscriptions() {
        return Collections.unmodifiableList(Arrays.asList(
                    subscription1, subscription2, subscription3, subscription4, subscription5,
                    subscription6, subscription7, subscription8, subscription9, subscription10,
                    subscription11, subscription12, subscription13, subscription14, subscription15,
                    subscription16, subscription17, subscription18, subscription19, subscription20 //etc...
            ).stream().filter(sub -> (sub != null && sub.getTopicName() != null)).collect(Collectors.toList()));
    }

    public boolean isEnabled() {
        return enabled;
    }
}
