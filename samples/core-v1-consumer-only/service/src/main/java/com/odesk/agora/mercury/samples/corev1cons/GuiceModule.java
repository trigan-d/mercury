package com.odesk.agora.mercury.samples.corev1cons;

import com.odesk.agora.mercury.consumer.SQSConsumerModule;

public class GuiceModule extends com.odesk.agora.guice.GuiceModule<Configuration> {

    @Override
    protected void configure() {
        super.configure();
        if(configuration.sqs.isEnabled()) {
            install(new SQSConsumerModule(configuration.sqs));
            bind(ExampleMessageConsumer.class).asEagerSingleton();
        }
    }

}
