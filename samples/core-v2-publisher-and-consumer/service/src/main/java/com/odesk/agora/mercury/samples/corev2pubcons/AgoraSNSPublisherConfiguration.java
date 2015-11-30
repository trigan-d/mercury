package com.odesk.agora.mercury.samples.corev2pubcons;

import com.odesk.agora.configuration.AwsServiceConfiguration;
import com.odesk.agora.mercury.publisher.SNSPublisherConfiguration;

import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by Dmitry Solovyov on 11/30/2015.
 */
public class AgoraSNSPublisherConfiguration extends AwsServiceConfiguration implements SNSPublisherConfiguration {
    @NotNull
    private String topicNames;

    @Override
    public Set<String> getTopics() {
        return Arrays.stream(topicNames.split(",")).map(String::trim).collect(Collectors.toSet());
    }
}
