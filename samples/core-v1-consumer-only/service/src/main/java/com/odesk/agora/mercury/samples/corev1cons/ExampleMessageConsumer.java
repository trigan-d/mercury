package com.odesk.agora.mercury.samples.corev1cons;

import com.google.inject.Inject;
import com.odesk.agora.mercury.consumer.MercuryMessage;
import com.odesk.agora.mercury.consumer.TopicMessagesRouter;
import com.yammer.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Dmitry Solovyov on 12/01/2015.
 */
public class ExampleMessageConsumer implements Managed {
    private static final Logger logger = LoggerFactory.getLogger(ExampleMessageConsumer.class);

    @Inject
    private TopicMessagesRouter router;

    @Override
    public void start() throws Exception {
        router.setTopicConsumer("MercuryTestCoreV1", this::logMessage);
    }

    @Override
    public void stop() throws Exception {
        router.removeTopicConsumer("MercuryTestCoreV1");
    }

    private void logMessage(MercuryMessage message) {
        logger.info("Received Mercury message {}", message);
    }
}