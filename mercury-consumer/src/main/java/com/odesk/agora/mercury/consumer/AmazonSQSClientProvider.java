package com.odesk.agora.mercury.consumer;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Provider;

/**
 * Created by Dmitry Solovyov on 11/27/2015.
 */
public class AmazonSQSClientProvider implements Provider<AmazonSQSClient> {
    private static final Logger logger = LoggerFactory.getLogger(AmazonSQSClientProvider.class);

    private AmazonSQSClient amazonSQSClient;

    public AmazonSQSClientProvider(SQSConsumerConfiguration configuration) {
        logger.info("Creating AmazonSQSClient() with config {}", configuration);

        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withConnectionTimeout(configuration.getConnectionTimeout())
                .withSocketTimeout(configuration.getSocketTimeout())
                .withMaxConnections(configuration.getMaxConnections())
                .withMaxErrorRetry(configuration.getMaxErrorRetry());

        final String awsAccessKey = configuration.getAccessKey();
        final String awsSecretKey = configuration.getSecretKey();

        if (Strings.isNullOrEmpty(awsAccessKey) || Strings.isNullOrEmpty(awsSecretKey)) {
            logger.info("Using IAM Roles for AmazonSQSClient()");
            amazonSQSClient = new AmazonSQSClient(new InstanceProfileCredentialsProvider(), clientConfiguration);
        } else {
            logger.info("Using provided AWS Credentials for AmazonSQSClient()");
            BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
            amazonSQSClient = new AmazonSQSClient(basicAWSCredentials, clientConfiguration);
        }
        logger.info("AmazonSQSClient client endpoint: {}", configuration.getEndpoint());
        amazonSQSClient.setEndpoint(configuration.getEndpoint());
    }

    public AmazonSQSClient get() {
        return amazonSQSClient;
    }
}
