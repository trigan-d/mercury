package com.odesk.agora.mercury.samples.corev2pubcons;

import com.odesk.agora.AgoraApplication;
import com.odesk.agora.configuration.Configuration;
import com.odesk.agora.guice.GuiceModule;
import com.odesk.agora.mercury.consumer.MessagesDispatcher;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Dmitry Solovyov on 11/30/2015.
 */
public class Service extends AgoraApplication<Configuration, GuiceModule> {
    private static final Logger logger = LoggerFactory.getLogger(Service.class);

    protected Service() {
        super("corev2pubcons", Configuration.class, null, new GuiceModule(), Resource.class);
    }

    public static void main(String[] args) throws Exception {
        new Service().run(args);
    }

    @Override
    public void run(Configuration configuration, Environment environment) throws Exception {
        super.run(configuration, environment);

        //Here we show just one of possible ways to register a topic consumer. Compare with core-v1 sample.
        getGuiceInjector().getInstance(MessagesDispatcher.class).setTopicConsumer("MercuryTestCoreV2", message -> logger.info("Received Mercury message {}", message));

        //getGuiceInjector().getInstance(MessagesDispatcher.class).setTopicConsumer("MercuryTestCoreV2", message -> { throw new RuntimeException("Message processing failed"); });
        //getGuiceInjector().getInstance(MessagesDispatcher.class).setTopicDlqConsumer("MercuryTestCoreV2", message -> logger.info("Received Mercury message {} from DLQ", message));
    }
}
