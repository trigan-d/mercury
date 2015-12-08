package com.odesk.agora.mercury.consumer;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import com.odesk.agora.mercury.MercuryMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * Created by Dmitry Solovyov on 11/27/2015.
 */
public class TopicQueueListener implements Runnable {
    private final Logger logger;

    private final TopicSubscriptionConfiguration topicConfig;
    private final AmazonSQSClient sqsClient;
    private final String queueUrl;
    private final MercuryMessagesDispatcher messagesDispatcher;
    private final ReceiveMessageRequest receiveMessageRequest;

    public TopicQueueListener(TopicSubscriptionConfiguration topicConfig, AmazonSQSClient sqsClient, String queueUrl, MercuryMessagesDispatcher messagesDispatcher) {
        this.topicConfig = topicConfig;
        this.sqsClient = sqsClient;
        this.queueUrl = queueUrl;
        this.messagesDispatcher = messagesDispatcher;
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
                    sqsSubject = body.tryGetString("Subject");
                    sqsMessage = body.tryGetString("Message");
                } catch (JSONException e) {
                    logger.error("Can't handle SNS message due to json error", e);
                    toDLQ.add(message);
                    return;
                }

                logger.debug("Processing Mercury message subject={}, message={}", sqsSubject, sqsMessage);

                try {
                    messagesDispatcher.route(new MercuryMessage(topicConfig.getTopicName(), sqsSubject, sqsMessage));
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
                List<DeleteMessageBatchRequestEntry> deleteRequestEntries = toDelete.stream()
                        .map(receiptHandle -> new DeleteMessageBatchRequestEntry(UUID.randomUUID().toString(), receiptHandle))
                        .collect(Collectors.toList());
                sqsClient.deleteMessageBatch(queueUrl, deleteRequestEntries);
                //TODO: handle messages that failed to delete.
            }
        }
    }
}
