package com.odesk.agora.mercury.sns;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.odesk.agora.mercury.aop.InvocationWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by Dmitry Solovyov on 11/23/2015.
 */
public class SNSClientBuilder {
    private static final Logger logger = LoggerFactory.getLogger(SNSClientBuilder.class);

    private SNSConfiguration configuration;
    private InvocationWrapper<PublishRequest, PublishResult> publishWrapper;

    public static SNSClientBuilder forConfig(SNSConfiguration configuration) {
        SNSClientBuilder builder = new SNSClientBuilder();
        builder.configuration = configuration;
        return builder;
    }

    public SNSClientBuilder withPublishWrapper(InvocationWrapper<PublishRequest, PublishResult> publishWrapper) {
        this.publishWrapper = publishWrapper;
        return this;
    }

    public AmazonSNSClient build() {
        logger.info("Creating AmazonSNSClient() with config {}", configuration);

        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withConnectionTimeout(configuration.getConnectionTimeout())
                .withSocketTimeout(configuration.getSocketTimeout())
                .withMaxConnections(configuration.getMaxConnections())
                .withMaxErrorRetry(configuration.getMaxErrorRetry());

        final String awsAccessKey = configuration.getAccessKey();
        final String awsSecretKey = configuration.getSecretKey();

        AWSCredentialsProvider credentialsProvider;

        if (awsAccessKey == null || awsAccessKey.isEmpty() || awsSecretKey == null || awsSecretKey.isEmpty()) {
            logger.info("Using IAM Roles for AmazonSNSClient()");
            credentialsProvider = new InstanceProfileCredentialsProvider();
        } else {
            logger.info("Using provided AWS Credentials for AmazonSNSClient()");
            BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
            credentialsProvider = new StaticCredentialsProvider(basicAWSCredentials);
        }

        AmazonSNSClient amazonSNSClient = new AmazonSNSClient(credentialsProvider, clientConfiguration) {
            @Override
            public PublishResult publish(PublishRequest publishRequest) {
                return publishWrapper == null ?
                        super.publish(publishRequest) :
                        publishWrapper.apply(super::publish, publishRequest);
            }
        };

        logger.info("AmazonSNSClient client endpoint: {}", configuration.getEndpoint());
        amazonSNSClient.setEndpoint(configuration.getEndpoint());

        return amazonSNSClient;
    }
}
