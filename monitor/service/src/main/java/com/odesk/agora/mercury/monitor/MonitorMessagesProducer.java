package com.odesk.agora.mercury.monitor;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.odesk.agora.mercury.MercuryMessage;
import com.odesk.agora.mercury.publisher.TopicPublisher;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by Dmitry Solovyov on 02/03/2016.
 */
public class MonitorMessagesProducer implements Managed {
    private static final Logger logger = LoggerFactory.getLogger(MonitorMessagesProducer.class);

    @Inject
    private Configuration configuration;

    @Inject @Named("MercuryMonitor")
    private TopicPublisher monitorTopicPublisher;

    @Inject
    private MonitorMessagesArchive messagesArchive;

    @Inject
    private MetricsProvider metricsProvider;

    private ScheduledExecutorService producerExecutor = Executors.newScheduledThreadPool(1);

    @Override
    public void start() throws Exception {
        producerExecutor.scheduleAtFixedRate(this::produceMessage, 0, configuration.getPublicationIntervalMillis(), TimeUnit.MILLISECONDS);
    }

    private void produceMessage() {
        try {
            MercuryMessage publishedMessage = monitorTopicPublisher.messageWithTextPayload("just another monitor message").publish();
            messagesArchive.savePublishedMessage(publishedMessage);
            logger.info("Published monitor message: {}", publishedMessage);
        } catch(Exception e) {
            logger.warn("Error while publishing monitor message", e);
        }
    }

    @Override
    public void stop() throws Exception {
        producerExecutor.shutdown();
    }
}
