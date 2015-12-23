package com.odesk.agora.mercury.consumer;

import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import com.odesk.agora.mercury.MercuryMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Dmitry Solovyov on 12/17/2015.
 */
public class DLQListener extends TopicQueueListener {
    public DLQListener(TopicSubscriptionConfiguration subscriptionConfig, AmazonSQSBufferedAsyncClient sqsClient, String queueUrl, MessagesDispatcher messagesDispatcher) {
        super(subscriptionConfig, sqsClient, queueUrl, messagesDispatcher);
    }

    @Override
    protected Logger createLogger() {
        return LoggerFactory.getLogger(TopicQueueListener.class.getName() + "-" + getTopicName() + "-DLQ");
    }

    @Override
    protected boolean checkConsumerExists() {
        if(messagesDispatcher.hasDlqConsumerForTopic(getTopicName())) {
            return true;
        } else {
            getLogger().info("No DLQ consumer registered for topic {} yet. Skipping SQS DLQ fetch.", getTopicName());
            return false;
        }
    }

    @Override
    protected void processParsedMessage(String sqsSubject, String sqsMessage) {
        messagesDispatcher.dispatchDlqMessage(new MercuryMessage(getTopicName(), sqsSubject, sqsMessage));
    }
}
