package com.odesk.agora.mercury.samples.corev1cons;

import com.odesk.agora.mercury.MercuryMessage;
import com.odesk.agora.mercury.consumer.MercuryConsumers;
import com.yammer.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Here we show just one of possible ways to register a topic consumer. Compare with core-v2 sample.
 */

/**
 * Created by Dmitry Solovyov on 12/01/2015.
 */
public class ExampleMessageConsumer implements Managed {
    private static final Logger logger = LoggerFactory.getLogger(ExampleMessageConsumer.class);

    @Override
    public void start() throws Exception {
        MercuryConsumers.setConsumer("MercuryTestCoreV1", this::logMessage);
    }

    @Override
    public void stop() throws Exception {
        MercuryConsumers.removeConsumer("MercuryTestCoreV1");
    }

    private void logMessage(MercuryMessage message) {
        logger.info("Received Mercury message {payload:'{}', content-type:'{}'}", message.getSerializedPayload(), message.getContentType());
    }
}