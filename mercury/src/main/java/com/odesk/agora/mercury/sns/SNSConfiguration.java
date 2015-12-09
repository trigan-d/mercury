package com.odesk.agora.mercury.sns;

import javax.validation.constraints.NotNull;
import java.util.Set;

/**
 * Created by Dmitry Solovyov on 11/23/2015.
 */
public interface SNSConfiguration {
    String getEndpoint();

    int getSocketTimeout();

    int getConnectionTimeout();

    int getMaxConnections();

    int getMaxErrorRetry();

    String getAccessKey();

    String getSecretKey();
}
