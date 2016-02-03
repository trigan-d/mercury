package com.odesk.agora.mercury.monitor;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;

import java.util.concurrent.TimeUnit;

/**
 * Created by Dmitry Solovyov on 02/03/2016.
 */
public class MetricsProvider {
    private final MetricsRegistry registry = Metrics.defaultRegistry();

    /* meters */
    private final Meter publicationFailedMeter;
    private final Meter deliveryFailedMeter;
    private final Meter messageHighLatencyMeter;

    /* timers */
    private final Timer messageLatencyMercuryTimer;

    public MetricsProvider() {
        /* meters */
        deliveryFailedMeter = registry.newMeter(MetricsProvider.class, "failedDeliveryRateForMercury", "failed", TimeUnit.SECONDS);
        publicationFailedMeter = registry.newMeter(MetricsProvider.class, "failedPublicationRateForMercury", "failed", TimeUnit.SECONDS);
        messageHighLatencyMeter = registry.newMeter(MetricsProvider.class, "highLatencyMessagesRateForMercury", "failed", TimeUnit.SECONDS);

        /* timers */
        messageLatencyMercuryTimer = registry.newTimer(MetricsProvider.class, "ratesAndLatenciesOfMessagePassingMercury", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    }

    public Meter getDeliveryFailedMeter() {
        return deliveryFailedMeter;
    }

    public Meter getMessageHighLatencyMeter() {
        return messageHighLatencyMeter;
    }

    public Timer getMessageLatencyMercuryTimer() {
        return messageLatencyMercuryTimer;
    }

    public Meter getPublicationFailedMeter() {
        return publicationFailedMeter;
    }
}
