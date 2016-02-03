package com.odesk.agora.mercury.monitor;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.odesk.agora.configuration.Configuration;
import com.odesk.agora.mercury.publisher.TopicPublisher;
import io.dropwizard.lifecycle.Managed;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by Dmitry Solovyov on 02/03/2016.
 */
public class MonitorMessagesProducer implements Managed {
    @Inject
    private Configuration configuration;

    @Inject @Named("MercuryMonitor")
    private TopicPublisher monitorTopicPublisher;

    private ScheduledExecutorService producerExecutor = Executors.newScheduledThreadPool(1);

    @Override
    public void start() throws Exception {
        long consumerPollingIntervalMs = configuration.getMercuryConfiguration().getConsumerConfiguration().getTopicSubscriptions().get(1).getPollingIntervalMs();
        producerExecutor.scheduleWithFixedDelay(this::produceMessage, 0, consumerPollingIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void produceMessage() {
        TopicPublisher.MessageToPublish message = monitorTopicPublisher.messageWithTextPayload("just another monitor event");
        message.publish();
    }

    @Override
    public void stop() throws Exception {
        producerExecutor.shutdown();
    }
}
