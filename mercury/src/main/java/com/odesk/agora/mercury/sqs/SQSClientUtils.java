package com.odesk.agora.mercury.sqs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import com.amazonaws.services.sqs.buffered.QueueBufferConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by Dmitry Solovyov on 11/27/2015.
 */
public class SQSClientUtils {
    private static final Logger logger = LoggerFactory.getLogger(SQSClientUtils.class);

    public static AmazonSQSBufferedAsyncClient buildAmazonSQSClient(SQSConfiguration configuration) {
        logger.info("Creating AmazonSQSClient() with config {}", configuration);

        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withConnectionTimeout(configuration.getConnectionTimeout())
                .withSocketTimeout(configuration.getSocketTimeout())
                .withMaxConnections(configuration.getMaxConnections())
                .withMaxErrorRetry(configuration.getMaxErrorRetry());

        final String awsAccessKey = configuration.getAccessKey();
        final String awsSecretKey = configuration.getSecretKey();

        AmazonSQSAsyncClient sqsAsync;

        if (awsAccessKey == null || awsAccessKey.isEmpty() || awsSecretKey == null || awsSecretKey.isEmpty()) {
            logger.info("Using IAM Roles for AmazonSQSClient()");
            sqsAsync = new AmazonSQSAsyncClient(new InstanceProfileCredentialsProvider(), clientConfiguration);
        } else {
            logger.info("Using provided AWS Credentials for AmazonSQSClient()");
            BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
            sqsAsync = new AmazonSQSAsyncClient(new StaticCredentialsProvider(basicAWSCredentials), clientConfiguration);
        }
        logger.info("AmazonSQSClient client endpoint: {}", configuration.getEndpoint());
        sqsAsync.setEndpoint(configuration.getEndpoint());

        QueueBufferConfig queueBufferConfig = new QueueBufferConfig().withMaxDoneReceiveBatches(0); //disable messages prefetching
        return new AmazonSQSBufferedAsyncClient(sqsAsync, queueBufferConfig);
    }
}
