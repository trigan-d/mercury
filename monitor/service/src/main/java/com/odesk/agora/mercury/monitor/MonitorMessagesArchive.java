package com.odesk.agora.mercury.monitor;

import com.google.inject.Inject;
import com.odesk.agora.mercury.MercuryMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Dmitry Solovyov on 02/03/2016.
 */
public class MonitorMessagesArchive {
    private static final Logger logger = LoggerFactory.getLogger(MonitorMessagesArchive.class);

    @Inject
    private Configuration configuration;

    @Inject
    private MetricsProvider metricsProvider;

    private final static ConcurrentHashMap<String, MercuryMessage> publishedMessages = new ConcurrentHashMap<>();

    public void savePublishedMessage(MercuryMessage message) {
        detectAndCleanFailedMessages();
        publishedMessages.put(message.getMessageId(), message);
    }

    public void removeConsumedMessage(MercuryMessage message) {
        publishedMessages.remove(message.getMessageId());
    }

    private void detectAndCleanFailedMessages() {
        for(Iterator<Map.Entry<String, MercuryMessage>> it = publishedMessages.entrySet().iterator(); it.hasNext(); ) {
            MercuryMessage message = it.next().getValue();
            if((System.currentTimeMillis() - message.getTimestamp().getTime()) > configuration.getMessageFailedLatencyMillis()) {
                logger.warn("Failed message detected: {}", message);
                metricsProvider.getDeliveryFailedMeter().mark();
                it.remove();
            }
        }
    }
}
