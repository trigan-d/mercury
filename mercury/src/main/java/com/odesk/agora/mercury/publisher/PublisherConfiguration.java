package com.odesk.agora.mercury.publisher;

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
    private String topicNamesPrefix;

    private String topicsForPublishing;

    public Set<String> getPublisherTopics() {
        return (topicsForPublishing == null || topicsForPublishing.isEmpty()) ?
                Collections.emptySet() :
                Arrays.stream(topicsForPublishing.split(",")).map(String::trim).collect(Collectors.toSet());
    }

    public String getTopicNamesPrefix() {
        return topicNamesPrefix;
    }

    public void setTopicNamesPrefix(String topicNamesPrefix) {
        this.topicNamesPrefix = topicNamesPrefix;
    }
}
