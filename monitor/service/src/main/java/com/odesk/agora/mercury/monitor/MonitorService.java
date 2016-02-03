package com.odesk.agora.mercury.monitor;

import com.odesk.agora.AgoraApplication;
import com.odesk.agora.configuration.Configuration;
import com.odesk.agora.guice.GuiceModule;
import com.odesk.agora.mercury.consumer.MercuryConsumers;
import com.odesk.agora.mercury.consumer.TypedMessage;
import com.odesk.agora.thrift.hello.THello;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Dmitry Solovyov on 03/02/2016.
 */
public class MonitorService extends AgoraApplication<Configuration, GuiceModule> {
    private static final Logger logger = LoggerFactory.getLogger(MonitorService.class);

    protected MonitorService() {
        super("merury-monitor", Configuration.class, null, new GuiceModule(), Resource.class);
    }

    public static void main(String[] args) throws Exception {
        new MonitorService().run(args);
    }

    @Override
    public void run(Configuration configuration, Environment environment) throws Exception {
        super.run(configuration, environment);

        environment.lifecycle().manage(getGuiceInjector().getInstance(MonitorMessagesProducer.class));
    }
}
