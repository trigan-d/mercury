package com.odesk.agora.mercury.sns;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.sns.AmazonSNSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by Dmitry Solovyov on 11/23/2015.
 */
public class SNSClientUtils {
    private static final Logger logger = LoggerFactory.getLogger(SNSClientUtils.class);

    public static AmazonSNSClient buildAmazonSNSClient(SNSConfiguration configuration) {
        logger.info("Creating AmazonSNSClient() with config {}", configuration);

        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withConnectionTimeout(configuration.getConnectionTimeout())
                .withSocketTimeout(configuration.getSocketTimeout())
                .withMaxConnections(configuration.getMaxConnections())
                .withMaxErrorRetry(configuration.getMaxErrorRetry());

        final String awsAccessKey = configuration.getAccessKey();
        final String awsSecretKey = configuration.getSecretKey();

        AmazonSNSClient amazonSNSClient;

        if (awsAccessKey == null || awsAccessKey.isEmpty() || awsSecretKey == null || awsSecretKey.isEmpty()) {
            logger.info("Using IAM Roles for AmazonSNSClient()");
            amazonSNSClient = new AmazonSNSClient(new InstanceProfileCredentialsProvider(), clientConfiguration);
        } else {
            logger.info("Using provided AWS Credentials for AmazonSNSClient()");
            BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
            amazonSNSClient = new AmazonSNSClient(basicAWSCredentials, clientConfiguration);
        }
        logger.info("AmazonSNSClient client endpoint: {}", configuration.getEndpoint());
        amazonSNSClient.setEndpoint(configuration.getEndpoint());

        return amazonSNSClient;
    }
}
