package com.odesk.agora.mercury.sns;


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
