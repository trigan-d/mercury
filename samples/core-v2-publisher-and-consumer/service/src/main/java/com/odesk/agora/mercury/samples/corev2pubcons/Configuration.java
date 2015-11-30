package com.odesk.agora.mercury.samples.corev2pubcons;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 * Created by Dmitry Solovyov on 11/30/2015.
 */
public class Configuration extends com.odesk.agora.configuration.Configuration {
    @Valid
    @NotNull
    public AgoraSQSConsumerConfiguration sqs;

    @Valid
    @NotNull
    public AgoraSNSPublisherConfiguration sns;
}
