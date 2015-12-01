package com.odesk.agora.mercury.samples.corev2pubcons;

import com.odesk.agora.mercury.consumer.SQSConsumerModule;
import com.odesk.agora.mercury.publisher.SNSPublisherModule;

/**
 * Created by Dmitry Solovyov on 11/30/2015.
 */
public class GuiceModule extends com.odesk.agora.guice.GuiceModule<Configuration> {
    @Override
    protected void configure() {
        super.configure();

        if(configuration.sqs.isEnabled()) {
            install(new SQSConsumerModule(configuration.sqs));
            bind(ExampleMessageConsumer.class).asEagerSingleton();
        }

        if(configuration.sns.isEnabled()) {
            install(new SNSPublisherModule(configuration.sns));
        }
    }
}
