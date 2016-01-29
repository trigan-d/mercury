package com.odesk.agora.mercury.consumer;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import com.amazonaws.util.json.Jackson;
import com.odesk.agora.mercury.MercuryMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Created by Dmitry Solovyov on 11/27/2015.
 */
public class TopicQueueListener implements Runnable {
    private final Logger logger;

    private final String topicName;
    private final String queueUrl;
    private final boolean isDLQ;

    private final AmazonSQSBufferedAsyncClient sqsClient;
    private final Executor consumptionExecutor;

    private final String topicNameForLogging;
    private final ReceiveMessageRequest receiveMessageRequest;
    private final AsyncHandler<DeleteMessageRequest, Void> deletionAsyncHandler;

    public TopicQueueListener(String topicName, String queueUrl, boolean isDLQ,
                              AmazonSQSBufferedAsyncClient sqsClient, Executor consumptionExecutor) {
        this.sqsClient = sqsClient;
        this.queueUrl = queueUrl;
        this.isDLQ = isDLQ;
        this.topicName = topicName;
        this.consumptionExecutor = consumptionExecutor;

        this.topicNameForLogging = topicName + (isDLQ ? "-DLQ" : "");

        logger = LoggerFactory.getLogger(TopicQueueListener.class.getName() + "-" + topicNameForLogging);

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
        if(MercuryConsumers.getConsumerForTopic(topicName, isDLQ) == null) {
            logger.warn("No consumer registered for {} yet. Skip SQS fetching.", topicNameForLogging);
        } else {
            ReceiveMessageResult pollResult = sqsClient.receiveMessage(receiveMessageRequest);

            if(! pollResult.getMessages().isEmpty()) {
                logger.info("Received {} messages", pollResult.getMessages().size());

                for(Message message : pollResult.getMessages()) {
                    consumptionExecutor.execute(new ConsumptionJob(message));
                };
            }
        }
    }


    public class ConsumptionJob implements Runnable {
        private final Message message;

        public ConsumptionJob(Message message) {
            this.message = message;
        }

        public String getTopicName() {
            return topicName;
        }

        @Override
        public void run() {
            Consumer<MercuryMessage> consumer = MercuryConsumers.getConsumerForTopic(topicName, isDLQ);

            if(consumer == null) {
                logger.error("No consumer found for {}. Skip message processing.", topicNameForLogging);
                return;
            }

            try {
                MercuryMessage mercuryMessage = Jackson.fromJsonString(message.getBody(), MercuryMessage.class);
                logger.debug("Processing message {}", mercuryMessage);
                consumer.accept(mercuryMessage);
            } catch (Throwable t) {
                logger.error("Can't process SQS message", t);
                return;
            }

            sqsClient.deleteMessageAsync(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()), deletionAsyncHandler);
        }
    }
}
