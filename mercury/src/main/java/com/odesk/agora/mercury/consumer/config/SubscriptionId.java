package com.odesk.agora.mercury.consumer.config;

import com.amazonaws.util.StringUtils;
import com.odesk.agora.mercury.consumer.ListenersRunner;

import javax.validation.constraints.NotNull;
import java.io.Serializable;

/**
 * Created by Dmitry Solovyov on 04/02/2016.
 * <p>
 * It is an unique identifier of Mercury subscription within a single application.
 * It consists of a Mercury topic's name and an optional name for subscribed SQS queue.
 * The queue name must be used if you want to subscribe several independently configured and maintained queues to the same topic in your application.
 * All queues subscribed to the same topic should have different names.
 * Please keep the queue names short, as SQS does not allow the full name to exceed 80 characters, and we still need to add a prefix and postfix to the one you give.
 * You can leave the queue name empty if you don't have other subscriptions to that topic.
 */
public class SubscriptionId implements Serializable {
    @NotNull
    private String topicName;

    private String queueName;

    public SubscriptionId() {
    }

    public SubscriptionId(String topicName, String queueName) {
        this.topicName = topicName;
        this.queueName = queueName;
    }

    public static SubscriptionId forTopic(String topicName) {
        return new SubscriptionId(topicName, null);
    }

    public static SubscriptionId forTopicWithQueue(String topicName, String queueName) {
        return new SubscriptionId(topicName, queueName);
    }

    public String getTopicName() {
        return topicName;
    }

    public String getQueueName() {
        return queueName;
    }

    @Override
    public String toString() {
        if(StringUtils.isNullOrEmpty(queueName)) {
            return topicName;
        } else {
            return topicName + ListenersRunner.QUEUE_NAME_DELIMITER + queueName;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        } else if(obj instanceof SubscriptionId) {
            return this.toString().equals(obj.toString());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }
}
