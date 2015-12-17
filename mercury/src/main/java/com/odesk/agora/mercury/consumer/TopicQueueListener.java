package com.odesk.agora.mercury.consumer;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import com.odesk.agora.mercury.MercuryMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by Dmitry Solovyov on 11/27/2015.
 */
public class TopicQueueListener implements Runnable {
    private final Logger logger;

    private final TopicSubscriptionConfiguration subscriptionConfig;
    private final AmazonSQSBufferedAsyncClient sqsClient;
    private final String queueUrl;
    private final MessagesDispatcher messagesDispatcher;

    private final ReceiveMessageRequest receiveMessageRequest;
    private final AsyncHandler<DeleteMessageRequest, Void> deletionAsyncHandler;

    public TopicQueueListener(TopicSubscriptionConfiguration subscriptionConfig, AmazonSQSBufferedAsyncClient sqsClient, String queueUrl, MessagesDispatcher messagesDispatcher) {
        this.subscriptionConfig = subscriptionConfig;
        this.sqsClient = sqsClient;
        this.queueUrl = queueUrl;
        this.messagesDispatcher = messagesDispatcher;

        logger = LoggerFactory.getLogger(TopicQueueListener.class.getName() + "-" + subscriptionConfig.getTopicName());

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
        if(! messagesDispatcher.hasConsumerForTopic(subscriptionConfig.getTopicName())) {
            logger.warn("No consumer registered for topic {} yet. Skipping SQS fetch.", subscriptionConfig.getTopicName());
            return;
        }

        ReceiveMessageResult pollResult = sqsClient.receiveMessage(receiveMessageRequest);

        if(! pollResult.getMessages().isEmpty()) {
            logger.info("Received {} messages", pollResult.getMessages().size());

            //TODO: should we allow to choose between parallel and non-parallel messages processing via TopicSubscriptionConfiguration?

            pollResult.getMessages().parallelStream().forEach(message -> {
                String sqsSubject;
                String sqsMessage;

                try {
                    JSONObject body = new JSONObject(message.getBody());
                    sqsSubject = body.tryGetString("Subject");
                    sqsMessage = body.tryGetString("Message");
                } catch (JSONException e) {
                    logger.error("Can't handle SNS message due to json error", e);
                    return;
                }

                logger.debug("Processing Mercury message subject={}, message={}", sqsSubject, sqsMessage);

                try {
                    messagesDispatcher.route(new MercuryMessage(subscriptionConfig.getTopicName(), sqsSubject, sqsMessage));
                } catch (Throwable t) {
                    logger.error("Can't process SQS message", t);
                    return;
                }

                sqsClient.deleteMessageAsync(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()), deletionAsyncHandler);
            });
        }
    }
}
