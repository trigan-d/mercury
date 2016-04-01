package com.odesk.agora.mercury.consumer;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.util.json.Jackson;
import com.odesk.agora.mercury.AgoraMDCData;
import com.odesk.agora.mercury.MercuryMessage;
import com.odesk.agora.mercury.consumer.config.SubscriptionId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Created by Dmitry Solovyov on 11/27/2015.
 * <p>
 * Listener (or poller) for SQS queue. Instances are created and managed by {@link ListenersRunner}.
 * Each message received is passed to the consumer registered at {@link MercuryConsumers}.
 * Don't poll messages from the queue until a proper consumer is registered.
 * <p>
 * Not intended to be used by end-users.
 */
public class TopicQueueListener implements Runnable {
    public final static int MAX_NUMBER_OF_MESSAGES_PER_POLL = 10; //no needs to override this setting

    private final Logger logger;

    private final SubscriptionId subscriptionId;
    private final String queueUrl;
    private final boolean isDLQ;

    private final AmazonSQSBufferedAsyncClient sqsClient;
    private final Executor consumptionExecutor;
    private final Consumer<AgoraMDCData> agoraMDCDataSetter;
    private final Runnable agoraMDCDataCleaner;
    private final ConsumerMetricsHandler metricsHandler;

    private final String subscriptionIdForLogging;
    private final ReceiveMessageRequest receiveMessageRequest;
    private final AsyncHandler<DeleteMessageRequest, Void> deletionAsyncHandler;

    public TopicQueueListener(SubscriptionId subscriptionId, String queueUrl, boolean isDLQ,
                              AmazonSQSBufferedAsyncClient sqsClient, Executor consumptionExecutor,
                              Consumer<AgoraMDCData> agoraMDCDataSetter, Runnable agoraMDCDataCleaner, ConsumerMetricsHandler metricsHandler) {
        this.sqsClient = sqsClient;
        this.queueUrl = queueUrl;
        this.isDLQ = isDLQ;
        this.subscriptionId = subscriptionId;
        this.consumptionExecutor = consumptionExecutor;
        this.agoraMDCDataSetter = agoraMDCDataSetter;
        this.agoraMDCDataCleaner = agoraMDCDataCleaner;
        this.metricsHandler = metricsHandler;

        this.subscriptionIdForLogging = subscriptionId + (isDLQ ? "-DLQ" : "");

        logger = LoggerFactory.getLogger(TopicQueueListener.class.getName() + "-" + subscriptionIdForLogging);

        receiveMessageRequest = new ReceiveMessageRequest().withQueueUrl(queueUrl).withMaxNumberOfMessages(MAX_NUMBER_OF_MESSAGES_PER_POLL);

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
        if(MercuryConsumers.getConsumerForSubscription(subscriptionId, isDLQ) == null && PlainSQSConsumers.getConsumerForSubscription(subscriptionId, isDLQ) == null) {
            logger.warn("No consumer registered for {} yet. Skip SQS fetching.", subscriptionIdForLogging);
        } else {
            List<Message> pollingResult;

            try {
                pollingResult = sqsClient.receiveMessage(receiveMessageRequest).getMessages();
                if(metricsHandler != null) {
                    metricsHandler.handlePollingSuccess(subscriptionIdForLogging, pollingResult.size());
                }
            } catch(Throwable t) {
                logger.warn("Polling error", t);
                if(metricsHandler != null) {
                    metricsHandler.handlePollingFail(subscriptionIdForLogging);
                }
                throw t;
            }

            if(pollingResult.isEmpty()) {
                if(metricsHandler != null) {
                    metricsHandler.handleEmptyPollingResult(subscriptionIdForLogging);
                }
            } else {
                logger.info("Received {} messages", pollingResult.size());

                if((pollingResult.size() == MAX_NUMBER_OF_MESSAGES_PER_POLL) && (metricsHandler != null)) {
                    metricsHandler.handleFullPollingResult(subscriptionIdForLogging);
                }

                for(Message message : pollingResult) {
                    consumptionExecutor.execute(new ConsumptionJob(message));
                }
            }
        }
    }


    public class ConsumptionJob implements Runnable {
        private final Message message;
        private final long deliveryTime;

        public ConsumptionJob(Message message) {
            this.message = message;
            this.deliveryTime = System.currentTimeMillis();
        }

        @Override
        public void run() {
            boolean processed = false;

            Consumer<Message> plainSQSConsumer = PlainSQSConsumers.getConsumerForSubscription(subscriptionId, isDLQ);

            if(plainSQSConsumer != null) {
                processed = processAsPlainSQSMessage(plainSQSConsumer);
            } else {
                Consumer<MercuryMessage> mercuryConsumer = MercuryConsumers.getConsumerForSubscription(subscriptionId, isDLQ);

                if(mercuryConsumer == null) {
                    logger.error("No consumer found for {}. Skip message processing.", subscriptionIdForLogging);
                } else {
                    processed = processAsMercuryMessage(mercuryConsumer);
                }
            }

            if(processed) {
                sqsClient.deleteMessageAsync(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()), deletionAsyncHandler);
            }
        }

        private boolean processAsPlainSQSMessage(Consumer<Message> plainSQSConsumer) {
            try {
                logger.debug("Processing plain SQS message {}", message);
                plainSQSConsumer.accept(message);
                handleMetricConsumptionSuccess();
                return true;
            } catch (Throwable t) {
                logger.error("Can't process plain SQS message", t);
                handleMetricConsumptionFail();
                return false;
            }
        }

        private boolean processAsMercuryMessage(Consumer<MercuryMessage> mercuryConsumer) {
            try {
                MercuryMessage mercuryMessage = Jackson.fromJsonString(message.getBody(), MercuryMessage.class);
                if(metricsHandler != null && !isDLQ) {
                    metricsHandler.handleDeliveryLatency(subscriptionIdForLogging, deliveryTime - mercuryMessage.getTimestamp().getTime());
                }

                if(mercuryMessage.getAgoraMDCData() != null) {
                    agoraMDCDataSetter.accept(mercuryMessage.getAgoraMDCData());
                }

                logger.debug("Processing message {}", mercuryMessage);
                mercuryConsumer.accept(mercuryMessage);
                handleMetricConsumptionSuccess();
                return true;
            } catch (Throwable t) {
                logger.error("Can't process Mercury message", t);
                handleMetricConsumptionFail();
                return false;
            } finally {
                agoraMDCDataCleaner.run();
            }
        }

        private void handleMetricConsumptionFail() {
            if(metricsHandler != null) {
                metricsHandler.handleConsumptionFail(subscriptionIdForLogging);
            }
        }

        private void handleMetricConsumptionSuccess() {
            if(metricsHandler != null) {
                metricsHandler.handleConsumptionDuration(subscriptionIdForLogging, System.currentTimeMillis() - deliveryTime);
            }
        }
    }
}
