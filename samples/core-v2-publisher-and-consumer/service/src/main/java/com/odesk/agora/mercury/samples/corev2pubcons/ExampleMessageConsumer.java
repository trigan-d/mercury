package com.odesk.agora.mercury.samples.corev2pubcons;

import com.google.inject.Inject;
import com.odesk.agora.mercury.consumer.MercuryMessage;
import com.odesk.agora.mercury.consumer.TopicMessagesRouter;
import com.odesk.agora.mercury.consumer.TopicSubscriptionConfiguration;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Dmitry Solovyov on 11/30/2015.
 */
public class ExampleMessageConsumer implements Managed {
    private static final Logger logger = LoggerFactory.getLogger(ExampleMessageConsumer.class);

    @Inject
    private TopicMessagesRouter router;

    @Inject
    private Configuration configuration;

    @Override
    public void start() throws Exception {
        for(TopicSubscriptionConfiguration subscription : configuration.sqs.getTopicSubscriptions()) {
            router.setTopicConsumer(subscription.getTopicName(), this::logMessage);
        }
    }

    @Override
    public void stop() throws Exception {
        for(TopicSubscriptionConfiguration subscription : configuration.sqs.getTopicSubscriptions()) {
            router.removeTopicConsumer(subscription.getTopicName());
        }
    }

    private void logMessage(MercuryMessage message) {
        logger.info("Received Mercury message {}", message);
    }
}
