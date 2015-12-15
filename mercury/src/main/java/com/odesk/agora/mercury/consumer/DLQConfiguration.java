package com.odesk.agora.mercury.consumer;

import javax.validation.constraints.NotNull;

/**
 * Created by Dmitry Solovyov on 12/14/2015.
 */
public class DLQConfiguration {
    @NotNull
    private boolean enabled = true;

    public boolean isEnabled() {
        return enabled;
    }
}
