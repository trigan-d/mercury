package com.odesk.agora.mercury.consumer;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.odesk.agora.mercury.MercuryMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Dmitry Solovyov on 11/27/2015.
 */
public class TopicQueueListener implements Runnable {
    private final Logger logger;

    protected final MessagesDispatcher messagesDispatcher;
    private final TopicSubscriptionConfiguration subscriptionConfig;
    private final AmazonSQSBufferedAsyncClient sqsClient;
    private final String queueUrl;

    private final ReceiveMessageRequest receiveMessageRequest;
    private final AsyncHandler<DeleteMessageRequest, Void> deletionAsyncHandler;

    public TopicQueueListener(TopicSubscriptionConfiguration subscriptionConfig, AmazonSQSBufferedAsyncClient sqsClient, String queueUrl, MessagesDispatcher messagesDispatcher) {
        this.subscriptionConfig = subscriptionConfig;
        this.sqsClient = sqsClient;
        this.queueUrl = queueUrl;
        this.messagesDispatcher = messagesDispatcher;

        logger = createLogger();

        receiveMessageRequest = new ReceiveMessageRequest().withQueueUrl(queueUrl).withMaxNumberOfMessages(10);

        deletionAsyncHandler = new AsyncHandler<DeleteMessageRequest, Void>() {
            @Override
            public void onError(Exception exception) {
                logger.warn("SQS message deletion failed", exception);
            }
            @Override
            public void onSuccess(DeleteMessageRequest request, Void aVoid) {
                //do nothing, the deletion succeeded
            }
        };
    }

    public void run() {
        if(checkConsumerExists()) {
            ReceiveMessageResult pollResult = sqsClient.receiveMessage(receiveMessageRequest);

            if(! pollResult.getMessages().isEmpty()) {
                logger.info("Received {} messages", pollResult.getMessages().size());

                for(Message message : pollResult.getMessages()) {
                    messagesDispatcher.consumeAsync(new ConsumptionRunnable(this, message));
                };
            }
        }
    }

    public String getTopicName() {
        return subscriptionConfig.getTopicName();
    }

    protected Logger getLogger() {
        return logger;
    }

    protected Logger createLogger() {
        return LoggerFactory.getLogger(TopicQueueListener.class.getName() + "-" + getTopicName());
    }

    protected boolean checkConsumerExists() {
        if(messagesDispatcher.hasConsumerForTopic(subscriptionConfig.getTopicName())) {
            return true;
        } else {
            logger.warn("No consumer registered for topic {} yet. Skipping SQS fetch.", getTopicName());
            return false;
        }
    }

    protected void processParsedMessage(String sqsSubject, String sqsMessage) {
        messagesDispatcher.dispatchMessage(new MercuryMessage(getTopicName(), sqsSubject, sqsMessage));
    }

    protected void deleteMessage(Message message) {
        sqsClient.deleteMessageAsync(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()), deletionAsyncHandler);
    }
}
