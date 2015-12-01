package com.odesk.agora.mercury.samples.corev1cons;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class Configuration extends com.odesk.agora.configuration.Configuration {

    @Valid
    @NotNull
    public AgoraSQSConsumerConfiguration sqs;
}
