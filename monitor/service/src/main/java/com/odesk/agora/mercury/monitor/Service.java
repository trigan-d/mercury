package com.odesk.agora.mercury.monitor;

import com.odesk.agora.AgoraApplication;
import com.odesk.agora.guice.GuiceModule;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Dmitry Solovyov on 03/02/2016.
 */
public class Service extends AgoraApplication<Configuration, GuiceModule> {
    private static final Logger logger = LoggerFactory.getLogger(Service.class);

    protected Service() {
        super("mercury-monitor", Configuration.class, null, new GuiceModule());
    }

    public static void main(String[] args) throws Exception {
        new Service().run(args);
    }

    @Override
    public void run(Configuration configuration, Environment environment) throws Exception {
        super.run(configuration, environment);

        environment.lifecycle().manage(getGuiceInjector().getInstance(ScheduledLatencyInspector.class));
    }
}
