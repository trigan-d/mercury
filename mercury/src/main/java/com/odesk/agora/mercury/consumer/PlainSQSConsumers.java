package com.odesk.agora.mercury.consumer;

import com.odesk.agora.mercury.MercuryMessage;
import com.amazonaws.services.sqs.model.Message;

import com.odesk.agora.mercury.consumer.config.SubscriptionId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;


/**
 * Created by Dmitry Solovyov on 02/02/2016.
 * <p>
 * A registry of plain SQS consumers for Mercury subscriptions. Acts much like {@link MercuryConsumers}, but intended for low-level SQS messages processing
 * without an attempt to extract {@link MercuryMessage}. So all the consumers registered here operate on {@link Message} directly.
 * Those consumers always take precedence over the ones registered at {@link MercuryConsumers} for the same subscriptions.
 */
public class PlainSQSConsumers {
    private static final Logger logger = LoggerFactory.getLogger(PlainSQSConsumers.class);

    private static final ConcurrentHashMap<SubscriptionId, Consumer<Message>> consumers = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<SubscriptionId, Consumer<Message>> dlqConsumers = new ConcurrentHashMap<>();

    /**
     * Get a main/DLQ consumer registered for the given subscription.
     */
    public static Consumer<Message> getConsumerForSubscription(SubscriptionId subscriptionId, boolean dlq) {
        return (dlq ? dlqConsumers : consumers).get(subscriptionId);
    }

    /**
     * Set a main consumer for the given subscription.
     */
    public static void setConsumer(SubscriptionId subscriptionId, Consumer<Message> consumer) {
        consumers.put(subscriptionId, consumer);
        logger.info("Registered a consumer for subscription {}: {}", subscriptionId, consumer);
    }

    /**
     * Set a DLQ consumer for the given subscription.
     * @see #setConsumer(SubscriptionId, Consumer) the same method for subscription queue
     */
    public static void setDlqConsumer(SubscriptionId subscriptionId, Consumer<Message> consumer) {
        dlqConsumers.put(subscriptionId, consumer);
        logger.info("Registered a DLQ consumer for subscription {}: {}", subscriptionId, consumer);
    }

    /**
     * Remove a main consumer for the given subscription
     */
    public static void removeConsumer(SubscriptionId subscriptionId) {
        consumers.remove(subscriptionId);
        logger.info("Consumer for subscription {} unregistered", subscriptionId);
    }

    /**
     * Remove a DLQ consumer for the given subscription
     */
    public static void removeDlqConsumer(SubscriptionId subscriptionId) {
        dlqConsumers.remove(subscriptionId);
        logger.info("DLQ consumer for subscription {} unregistered", subscriptionId);
    }
}
