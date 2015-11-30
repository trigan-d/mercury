package com.odesk.agora.mercury.publisher;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.google.common.base.Strings;
import com.google.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by Dmitry Solovyov on 11/23/2015.
 */
public class AmazonSNSClientProvider implements Provider<AmazonSNSClient> {
    private static final Logger logger = LoggerFactory.getLogger(AmazonSNSClientProvider.class);

    private AmazonSNSClient amazonSNSClient;

    public AmazonSNSClientProvider(SNSPublisherConfiguration configuration) {
        logger.info("Creating AmazonSNSClient() with config {}", configuration);

        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withConnectionTimeout(configuration.getConnectionTimeout())
                .withSocketTimeout(configuration.getSocketTimeout())
                .withMaxConnections(configuration.getMaxConnections())
                .withMaxErrorRetry(configuration.getMaxErrorRetry());

        final String awsAccessKey = configuration.getAccessKey();
        final String awsSecretKey = configuration.getSecretKey();

        if (Strings.isNullOrEmpty(awsAccessKey) || Strings.isNullOrEmpty(awsSecretKey)) {
            logger.info("Using IAM Roles for AmazonSNSClient()");
            amazonSNSClient = new AmazonSNSClient(new InstanceProfileCredentialsProvider(), clientConfiguration);
        } else {
            logger.info("Using provided AWS Credentials for AmazonSNSClient()");
            BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
            amazonSNSClient = new AmazonSNSClient(basicAWSCredentials, clientConfiguration);
        }
        logger.info("AmazonSNSClient client endpoint: {}", configuration.getEndpoint());
        amazonSNSClient.setEndpoint(configuration.getEndpoint());
    }

    public AmazonSNSClient get() {
        return amazonSNSClient;
    }
}
