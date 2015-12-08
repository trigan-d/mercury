package com.odesk.agora.mercury.sqs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by Dmitry Solovyov on 11/27/2015.
 */
public class SQSClientUtils {
    private static final Logger logger = LoggerFactory.getLogger(SQSClientUtils.class);

    public static AmazonSQSClient buildAmazonSQSClient(SQSConfiguration configuration) {
        logger.info("Creating AmazonSQSClient() with config {}", configuration);

        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withConnectionTimeout(configuration.getConnectionTimeout())
                .withSocketTimeout(configuration.getSocketTimeout())
                .withMaxConnections(configuration.getMaxConnections())
                .withMaxErrorRetry(configuration.getMaxErrorRetry());

        final String awsAccessKey = configuration.getAccessKey();
        final String awsSecretKey = configuration.getSecretKey();

        AmazonSQSClient amazonSQSClient;

        if (awsAccessKey == null || awsAccessKey.isEmpty() || awsSecretKey == null || awsSecretKey.isEmpty()) {
            logger.info("Using IAM Roles for AmazonSQSClient()");
            amazonSQSClient = new AmazonSQSClient(new InstanceProfileCredentialsProvider(), clientConfiguration);
        } else {
            logger.info("Using provided AWS Credentials for AmazonSQSClient()");
            BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
            amazonSQSClient = new AmazonSQSClient(basicAWSCredentials, clientConfiguration);
        }
        logger.info("AmazonSQSClient client endpoint: {}", configuration.getEndpoint());
        amazonSQSClient.setEndpoint(configuration.getEndpoint());

        return amazonSQSClient;
    }
}
