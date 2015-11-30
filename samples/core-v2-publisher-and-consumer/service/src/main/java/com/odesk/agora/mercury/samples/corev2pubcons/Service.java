package com.odesk.agora.mercury.samples.corev2pubcons;

import com.odesk.agora.AgoraApplication;
import io.dropwizard.setup.Environment;

/**
 * Created by Dmitry Solovyov on 11/30/2015.
 */
public class Service extends AgoraApplication<Configuration, GuiceModule> {

    protected Service() {
        super("corev2pubcons", Configuration.class, null, new GuiceModule(), Resource.class);
    }

    public static void main(String[] args) throws Exception {
        new Service().run(args);
    }

    @Override
    public void run(Configuration configuration, Environment environment) throws Exception {
        super.run(configuration, environment);
        environment.lifecycle().manage(getGuiceInjector().getInstance(MessagesLogger.class));
    }
}
