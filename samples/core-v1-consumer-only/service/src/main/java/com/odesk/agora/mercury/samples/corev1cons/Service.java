package com.odesk.agora.mercury.samples.corev1cons;

import com.odesk.agora.mercury.consumer.TopicMessagesRouter;
import com.yammer.dropwizard.config.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Service extends com.odesk.agora.Service<Configuration, GuiceModule> {
    private static final Logger logger = LoggerFactory.getLogger(Service.class);

    public static void main(String[] args) throws Exception {
        new Service().run(args);
    }

    protected Service() {
        super("corev1cons", Configuration.class, null, new GuiceModule());
    }

    @Override
    public void run(Configuration configuration, final Environment environment) throws Exception {
        super.run(configuration, environment);

        //Here we show just one of possible ways to register a topic consumer. Compare with core-v2 sample.
        environment.manage(guiceInjector.getInstance(ExampleMessageConsumer.class));
        guiceInjector.getInstance(TopicMessagesRouter.class).setTopicConsumer("MercuryTestCoreV1", message -> logger.info("Received Mercury message {}", message));
    }
}
