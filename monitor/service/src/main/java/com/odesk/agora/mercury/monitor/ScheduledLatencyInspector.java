package com.odesk.agora.mercury.monitor;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalNotification;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.odesk.agora.mercury.MercuryMessage;
import com.odesk.agora.mercury.consumer.MercuryConsumers;
import com.odesk.agora.mercury.publisher.TopicPublisher;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Environment;
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
    private static final String TOPIC_NAME = "MercuryMonitor";

    @Inject
    private Configuration configuration;

    @Inject
    Environment environment;

    @Inject @Named(TOPIC_NAME)
    private TopicPublisher monitorTopicPublisher;

    private Cache<String, MercuryMessage> publishedMessagesArchive;
    private ScheduledExecutorService publisherExecutor;

    private Timer deliveryLatencyTimer;
    private Meter publicationFailedMeter;
    private Meter deliveryHighLatencyMeter;
    private Meter deliveryFailedMeter;

    @Override
    public void start() throws Exception {
        deliveryLatencyTimer = environment.metrics().timer(MetricRegistry.name(ScheduledLatencyInspector.class, "deliveryRatesAndLatencies"));
        publicationFailedMeter = environment.metrics().meter(MetricRegistry.name(ScheduledLatencyInspector.class, "failedPublicationRate"));
        deliveryHighLatencyMeter = environment.metrics().meter(MetricRegistry.name(ScheduledLatencyInspector.class, "highLatencyMessagesRate"));
        deliveryFailedMeter = environment.metrics().meter(MetricRegistry.name(ScheduledLatencyInspector.class, "failedDeliveryRate"));

        publishedMessagesArchive = CacheBuilder.newBuilder()
                .expireAfterWrite(configuration.getFailedDeliveryLatencyMillis(), TimeUnit.MILLISECONDS)
                .removalListener(this::onMessageRemovedFromArchive).build();

        publisherExecutor = Executors.newScheduledThreadPool(1);
        publisherExecutor.scheduleAtFixedRate(this::publishMessage,
                configuration.getPublicationIntervalMillis(), configuration.getPublicationIntervalMillis(), TimeUnit.MILLISECONDS);

        MercuryConsumers.setConsumer(TOPIC_NAME, this::consumeMessage);

        logger.info("Latency inspector started. PublicationInterval={}ms, HighLatencyThreshold={}ms, FailedDeliveryLatency={}ms.",
                configuration.getPublicationIntervalMillis(), configuration.getHighLatencyThresholdMillis(), configuration.getFailedDeliveryLatencyMillis());
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

        if(publishedMessagesArchive.getIfPresent(message.getMessageId()) == null) {
            logger.info("Received a monitor message that wasn't published in this session, or is already considered failed: {}", message);
        } else {
            deliveryLatencyTimer.update(latencyMillis, TimeUnit.MILLISECONDS);

            String logMsg = "Received monitor message: {}. Latency={}ms";

            if(latencyMillis > configuration.getHighLatencyThresholdMillis()) {
                deliveryHighLatencyMeter.mark();
                logMsg += ", outdated!";
            }

            logger.info(logMsg, message, latencyMillis);

            publishedMessagesArchive.invalidate(message.getMessageId());
        }
    }

    private void onMessageRemovedFromArchive(RemovalNotification<String, MercuryMessage> notification) {
        if(RemovalCause.EXPIRED.equals(notification.getCause())) {
            logger.warn("Delivery failed for monitor message {}", notification.getValue());
            deliveryFailedMeter.mark();
        }
    }
}
