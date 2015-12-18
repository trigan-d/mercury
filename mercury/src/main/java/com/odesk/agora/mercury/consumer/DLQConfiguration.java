package com.odesk.agora.mercury.consumer;

import javax.validation.constraints.NotNull;

/**
 * Created by Dmitry Solovyov on 12/14/2015.
 */
public class DLQConfiguration extends QueueConfiguration {
    private static final long DEFAULT_POLLING_INTEVAL_MS = 10000;
    private static final int DEFAULT_VISIBILITY_TIMEOUT_SEC = 60;
    private static final int DEFAULT_POLLING_WAIT_TIME_SEC = 20; //Enable long polling by default. Set to zero if you want short polling instead.
    private static final int DEFAULT_DELAY_SEC = 0;

    public DLQConfiguration() {
        super(DEFAULT_POLLING_INTEVAL_MS, DEFAULT_VISIBILITY_TIMEOUT_SEC, DEFAULT_POLLING_WAIT_TIME_SEC, DEFAULT_DELAY_SEC);
    }

    @NotNull
    private boolean enabled = true; //Could be overridden by configuration.

    public boolean isEnabled() {
        return enabled;
    }

}
