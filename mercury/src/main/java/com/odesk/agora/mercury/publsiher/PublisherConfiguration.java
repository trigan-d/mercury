package com.odesk.agora.mercury.publsiher;

import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by Dmitry Solovyov on 12/07/2015.
 */
public class PublisherConfiguration {
    @NotNull
    private boolean enabled;

    private String topicNames;

    public String getTopicNames() {
        return topicNames;
    }

    public Set<String> getTopicNamesSet() {
        return (topicNames == null || topicNames.isEmpty()) ?
                Collections.emptySet() :
                Arrays.stream(topicNames.split(",")).map(String::trim).collect(Collectors.toSet());
    }

    public boolean isEnabled() {
        return enabled;
    }
}
