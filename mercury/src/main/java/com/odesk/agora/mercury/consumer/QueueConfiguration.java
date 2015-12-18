package com.odesk.agora.mercury.consumer;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.HashMap;

/**
 * Created by Dmitry Solovyov on 11/25/2015.
 */
public abstract class QueueConfiguration {
    @NotNull
    @Min(100)
    private long pollingIntervalMs;

    @NotNull
    @Min(0)
    @Max(43200)
    private int visibilityTimeoutSec;

    @NotNull
    @Min(0)
    @Max(20)
    private int pollingWaitTimeSec;

    @NotNull
    @Min(0)
    @Max(900)
    private int delaySec;

    protected QueueConfiguration(long pollingIntervalMs, int visibilityTimeoutSec, int pollingWaitTimeSec, int delaySec) {
        this.pollingIntervalMs = pollingIntervalMs;
        this.visibilityTimeoutSec = visibilityTimeoutSec;
        this.pollingWaitTimeSec = pollingWaitTimeSec;
        this.delaySec = delaySec;
    }

    public HashMap<String, String> asSQSQueueAttributes() {
        HashMap<String, String> map = new HashMap<>();

        map.put("VisibilityTimeout", String.valueOf(visibilityTimeoutSec));
        map.put("ReceiveMessageWaitTimeSeconds", String.valueOf(pollingWaitTimeSec));
        map.put("DelaySeconds", String.valueOf(delaySec));

        return map;
    }

    public long getPollingIntervalMs() {
        return pollingIntervalMs;
    }

    public int getVisibilityTimeoutSec() {
        return visibilityTimeoutSec;
    }

    public int getPollingWaitTimeSec() {
        return pollingWaitTimeSec;
    }

    public int getDelaySec() {
        return delaySec;
    }
}
