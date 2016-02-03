package com.odesk.agora.mercury.monitor;

/**
 * Created by Dmitry Solovyov on 02/03/2016.
 */
public class GuiceModule extends com.odesk.agora.guice.GuiceModule<Configuration> {
    @Override
    protected void configure() {
        super.configure();

        bind(MonitorMessagesProducer.class).asEagerSingleton();
        bind(MonitorMessagesConsumer.class).asEagerSingleton();

        bind(MonitorMessagesArchive.class).asEagerSingleton();

        bind(MetricsProvider.class).asEagerSingleton();
    }
}
