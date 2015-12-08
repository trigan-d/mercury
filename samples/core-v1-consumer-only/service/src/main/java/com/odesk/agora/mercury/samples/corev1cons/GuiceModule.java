package com.odesk.agora.mercury.samples.corev1cons;


import com.odesk.agora.configuration.Configuration;

public class GuiceModule extends com.odesk.agora.guice.GuiceModule<Configuration> {

    @Override
    protected void configure() {
        super.configure();

        bind(ExampleMessageConsumer.class).asEagerSingleton();
    }

}
