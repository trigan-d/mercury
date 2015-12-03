package com.odesk.agora.mercury.samples.corev1pub;

import com.odesk.agora.configuration.Configuration;
import com.odesk.agora.guice.GuiceModule;
import com.yammer.dropwizard.config.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Service extends com.odesk.agora.Service<Configuration, GuiceModule> {
    private static final Logger logger = LoggerFactory.getLogger(Service.class);

    public static void main(String[] args) throws Exception {
        new Service().run(args);
    }

    protected Service() {
        super("corev1cons", Configuration.class, null, new GuiceModule(), Resource.class);
    }
}
