package com.odesk.agora.mercury.monitor;

import com.odesk.agora.configuration.Configuration;
import com.odesk.agora.guice.GuiceModule;

/**
 * Created by Dmitry Solovyov on 02/03/2016.
 */
public class MonitorGuiceModule extends GuiceModule<Configuration> {
    @Override
    protected void configure() {
        super.configure();

        bind(MonitorMessagesProducer.class).asEagerSingleton();
    }
}
