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
    private long highLatencyThresholdMillis;

    @Min(10000)
    @NotNull
    private long failedDeliveryLatencyMillis;

    public long getHighLatencyThresholdMillis() {
        return highLatencyThresholdMillis;
    }

    public long getPublicationIntervalMillis() {
        return publicationIntervalMillis;
    }

    public long getFailedDeliveryLatencyMillis() {
        return failedDeliveryLatencyMillis;
    }
}
