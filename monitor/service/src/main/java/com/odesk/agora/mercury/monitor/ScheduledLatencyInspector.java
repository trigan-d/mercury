package com.odesk.agora.mercury.monitor;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalNotification;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.odesk.agora.mercury.MercuryMessage;
import com.odesk.agora.mercury.consumer.MercuryConsumers;
import com.odesk.agora.mercury.publisher.TopicPublisher;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by Dmitry Solovyov on 02/03/2016.
 */
public class ScheduledLatencyInspector implements Managed {
    private static final Logger logger = LoggerFactory.getLogger(ScheduledLatencyInspector.class);

    @Inject
    private Configuration configuration;

    @Inject @Named("MercuryMonitor")
    private TopicPublisher monitorTopicPublisher;

    private Cache<String, MercuryMessage> publishedMessagesArchive;
    private ScheduledExecutorService publisherExecutor;

    private Meter publicationFailedMeter;
    private Meter deliveryFailedMeter;
    private Meter deliveryHighLatencyMeter;
    private Timer deliveryLatencyTimer;

    @Override
    public void start() throws Exception {
        deliveryFailedMeter = Metrics.defaultRegistry().newMeter(ScheduledLatencyInspector.class, "failedDeliveryRate", "failed", TimeUnit.SECONDS);
        publicationFailedMeter = Metrics.defaultRegistry().newMeter(ScheduledLatencyInspector.class, "failedPublicationRate", "failed", TimeUnit.SECONDS);
        deliveryHighLatencyMeter = Metrics.defaultRegistry().newMeter(ScheduledLatencyInspector.class, "highLatencyMessagesRate", "failed", TimeUnit.SECONDS);
        deliveryLatencyTimer = Metrics.defaultRegistry().newTimer(ScheduledLatencyInspector.class, "deliveryRatesAndLatencies", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

        publishedMessagesArchive = CacheBuilder.newBuilder()
                .expireAfterWrite(configuration.getMessageFailedLatencyMillis(), TimeUnit.MILLISECONDS)
                .removalListener(this::onMessageRemovedFromArchive).build();

        publisherExecutor = Executors.newScheduledThreadPool(1);
        publisherExecutor.scheduleAtFixedRate(this::publishMessage,
                configuration.getPublicationIntervalMillis(), configuration.getPublicationIntervalMillis(), TimeUnit.MILLISECONDS);

        MercuryConsumers.setConsumer("MercuryMonitor", this::consumeMessage);
    }

    @Override
    public void stop() throws Exception {
        publisherExecutor.shutdown();
        MercuryConsumers.removeConsumer("MercuryMonitor");
    }

    private void publishMessage() {
        try {
            MercuryMessage publishedMessage = monitorTopicPublisher.messageWithTextPayload("just another monitor message").publish();

            publishedMessagesArchive.put(publishedMessage.getMessageId(), publishedMessage);
            logger.info("Published monitor message: {}", publishedMessage);
        } catch(Exception e) {
            logger.warn("Error while publishing monitor message", e);
            publicationFailedMeter.mark();
        }
    }

    private void consumeMessage(MercuryMessage message) {
        final long latencyMillis = System.currentTimeMillis() - message.getTimestamp().getTime();
        boolean isOutdated = latencyMillis > configuration.getMessageFailedLatencyMillis();

        logger.info("Received monitor message: {}. Latency={}ms (outdated={}).", message, latencyMillis, isOutdated);

        deliveryLatencyTimer.update(latencyMillis, TimeUnit.MILLISECONDS);
        if(isOutdated) {
            deliveryHighLatencyMeter.mark();
        }

        publishedMessagesArchive.invalidate(message.getMessageId());
    }

    private void onMessageRemovedFromArchive(RemovalNotification<String, MercuryMessage> notification) {
        if(RemovalCause.EXPIRED.equals(notification.getCause())) {
            logger.warn("Failed message detected: {}", notification.getValue());
            deliveryFailedMeter.mark();
        }
    }
}
