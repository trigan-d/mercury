package com.odesk.agora.mercury.monitor;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * Created by Dmitry Solovyov on 02/03/2016.
 */
public class Configuration extends com.odesk.agora.configuration.Configuration{
    @Valid
    @Min(100)
    @NotNull
    private long publicationIntervalMillis;

    @Min(1000)
    @NotNull
    private long messageFailedLatencyMillis;

    public long getMessageFailedLatencyMillis() {
        return messageFailedLatencyMillis;
    }

    public long getPublicationIntervalMillis() {
        return publicationIntervalMillis;
    }
}
