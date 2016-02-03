package com.odesk.agora.mercury.monitor;

import com.google.inject.Inject;
import com.odesk.agora.mercury.MercuryMessage;
import com.odesk.agora.mercury.consumer.MercuryConsumers;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Created by Dmitry Solovyov on 02/03/2016.
 */
public class MonitorMessagesConsumer implements Managed, Consumer<MercuryMessage> {
    private static final Logger logger = LoggerFactory.getLogger(MonitorMessagesConsumer.class);

    @Inject
    private MetricsProvider metricsProvider;

    @Inject
    private Configuration configuration;

    @Inject
    private MonitorMessagesArchive messagesArchive;

    @Override
    public void start() throws Exception {
        MercuryConsumers.setConsumer("MercuryMonitor", this);
    }

    @Override
    public void stop() throws Exception {
        MercuryConsumers.removeConsumer("MercuryMonitor");
    }

    @Override
    public void accept(MercuryMessage message) {
        final long latencyMillis = System.currentTimeMillis() - message.getTimestamp().getTime();
        boolean isOutdated = latencyMillis > configuration.getMessageFailedLatencyMillis();

        logger.info("Received monitor message: {}. Latency={}ms (outdated={}).", message, latencyMillis, isOutdated);

        metricsProvider.getMessageLatencyMercuryTimer().update(latencyMillis, TimeUnit.MILLISECONDS);
        if(isOutdated) {
            metricsProvider.getMessageHighLatencyMeter().mark();
        }

        messagesArchive.removeConsumedMessage(message);
    }
}
