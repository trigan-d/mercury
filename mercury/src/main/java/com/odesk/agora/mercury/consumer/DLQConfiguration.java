package com.odesk.agora.mercury.consumer;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * Created by Dmitry Solovyov on 12/14/2015.
 */
public class DLQConfiguration {
    @NotNull
    private boolean enabled = true; //Could be overridden by configuration.

    @NotNull
    @Min(100)
    private long pollingIntervalMs = 10000; //Default polling interval for DLQ. Could be overridden by configuration.

    public boolean isEnabled() {
        return enabled;
    }

    public long getPollingIntervalMs() {
        return pollingIntervalMs;
    }
}
