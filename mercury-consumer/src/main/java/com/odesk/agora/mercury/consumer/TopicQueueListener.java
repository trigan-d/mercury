package com.odesk.agora.mercury.consumer;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by Dmitry Solovyov on 11/27/2015.
 */
public class TopicQueueListener implements Runnable {
    private final Logger logger;

    private final TopicSubscriptionConfiguration topicConfig;
    private final AmazonSQSClient sqsClient;
    private final String queueUrl;
    private final TopicMessagesRouter messagesRouter;
    private final ReceiveMessageRequest receiveMessageRequest;

    public TopicQueueListener(TopicSubscriptionConfiguration topicConfig, AmazonSQSClient sqsClient, String queueUrl, TopicMessagesRouter messagesRouter) {
        this.topicConfig = topicConfig;
        this.sqsClient = sqsClient;
        this.queueUrl = queueUrl;
        this.messagesRouter = messagesRouter;
        logger = LoggerFactory.getLogger(TopicQueueListener.class.getName() + "-" + topicConfig.getTopicName());
        receiveMessageRequest = new ReceiveMessageRequest().withQueueUrl(queueUrl).withMaxNumberOfMessages(topicConfig.getPollingBatchSize());
    }

    public void run() {
        ReceiveMessageResult pollResult = sqsClient.receiveMessage(receiveMessageRequest);

        if(! pollResult.getMessages().isEmpty()) {
            logger.info("Received {} messages", pollResult.getMessages().size());

            final ConcurrentLinkedQueue<Message> toDLQ = new ConcurrentLinkedQueue<>();
            final ConcurrentLinkedQueue<String> toDelete = new ConcurrentLinkedQueue<>();

            //TODO: should we allow to choose between parallel and non-parallel messages processing via TopicSubscriptionConfiguration?

            pollResult.getMessages().parallelStream().forEach(message -> {
                String sqsSubject;
                String sqsMessage;

                try {
                    JSONObject body = new JSONObject(message.getBody());
                    sqsSubject = body.getString("Subject");
                    sqsMessage = body.getString("Message");
                } catch (JSONException e) {
                    logger.error("Can't handle SNS message due to json error", e);
                    toDLQ.add(message);
                    return;
                }

                logger.debug("Processing Mercury message subject={}, message={}", sqsSubject, sqsMessage);

                try {
                    messagesRouter.route(new MercuryMessage(topicConfig.getTopicName(), sqsSubject, sqsMessage));
                } catch (Throwable t) {
                    logger.error("Can't process SQS message", t);
                    toDLQ.add(message);
                    return;
                }

                toDelete.add(message.getReceiptHandle());
            });

            if(!toDLQ.isEmpty()) {
                //TODO: process the "toDLQ" messages list. Send them to DLQ. This option is provided by SQS.
            }

            if(!toDelete.isEmpty()) {
                //TODO: is it OK to use numbers as ids of DeleteMessageBatchRequestEntry? SQS documentation says only that the ids should be unique within the single batch delete request.
                ArrayList<DeleteMessageBatchRequestEntry> deleteRequestEntries = new ArrayList<>();
                for(int i=0; i<toDelete.size(); i++) {
                    deleteRequestEntries.add(new DeleteMessageBatchRequestEntry(String.valueOf(i), toDelete.poll()));
                }
                sqsClient.deleteMessageBatch(queueUrl, deleteRequestEntries);
                //TODO: handle messages that failed to delete.
            }
        }
    }
}
