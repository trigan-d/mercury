package com.odesk.agora.mercury.publisher;

import javax.validation.constraints.NotNull;
import java.util.Set;

/**
 * Created by Dmitry Solovyov on 11/23/2015.
 */
public interface SNSPublisherConfiguration {
    boolean isEnabled();

    String getEndpoint();

    int getSocketTimeout();

    int getConnectionTimeout();

    int getMaxConnections();

    int getMaxErrorRetry();

    String getAccessKey();

    String getSecretKey();

    @NotNull
    Set<String> getTopics();
}
