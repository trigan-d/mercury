package com.odesk.agora.mercury.sqs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import com.amazonaws.services.sqs.buffered.QueueBufferConfig;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.odesk.agora.mercury.aop.InvocationWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by Dmitry Solovyov on 11/27/2015.
 */
public class SQSClientBuilder {
    private static final Logger logger = LoggerFactory.getLogger(SQSClientBuilder.class);

    private SQSConfiguration configuration;
    private InvocationWrapper<ReceiveMessageRequest, ReceiveMessageResult> receiveWrapper;
    private InvocationWrapper<DeleteMessageBatchRequest, DeleteMessageBatchResult> deleteBatchWrapper;
    private InvocationWrapper<DeleteMessageRequest, Void> deleteWrapper;

    public static SQSClientBuilder forConfig(SQSConfiguration configuration) {
        SQSClientBuilder builder = new SQSClientBuilder();
        builder.configuration = configuration;
        return builder;
    }

    public SQSClientBuilder withReceiveWrapper(InvocationWrapper<ReceiveMessageRequest, ReceiveMessageResult> receiveWrapper) {
        this.receiveWrapper = receiveWrapper;
        return this;
    }

    public SQSClientBuilder withDeleteBatchWrapper(InvocationWrapper<DeleteMessageBatchRequest, DeleteMessageBatchResult> deleteBatchWrapper) {
        this.deleteBatchWrapper = deleteBatchWrapper;
        return this;
    }

    public SQSClientBuilder withDeleteWrapper(InvocationWrapper<DeleteMessageRequest, Void> deleteWrapper) {
        this.deleteWrapper = deleteWrapper;
        return this;
    }

    public AmazonSQSBufferedAsyncClient build() {
        logger.info("Creating AmazonSQSClient() with config {}", configuration);

        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withConnectionTimeout(configuration.getConnectionTimeout())
                .withSocketTimeout(configuration.getSocketTimeout())
                .withMaxConnections(configuration.getMaxConnections())
                .withMaxErrorRetry(configuration.getMaxErrorRetry());

        final String awsAccessKey = configuration.getAccessKey();
        final String awsSecretKey = configuration.getSecretKey();

        AWSCredentialsProvider credentialsProvider;

        if (awsAccessKey == null || awsAccessKey.isEmpty() || awsSecretKey == null || awsSecretKey.isEmpty()) {
            logger.info("Using IAM Roles for AmazonSQSClient()");
            credentialsProvider = new InstanceProfileCredentialsProvider();
        } else {
            logger.info("Using provided AWS Credentials for AmazonSQSClient()");
            BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
            credentialsProvider = new StaticCredentialsProvider(basicAWSCredentials);
        }

        AmazonSQSAsyncClient sqsAsync = new AmazonSQSAsyncClient(credentialsProvider, clientConfiguration) {
            @Override
            public ReceiveMessageResult receiveMessage(ReceiveMessageRequest receiveMessageRequest) {
                return receiveWrapper == null ?
                        super.receiveMessage(receiveMessageRequest) :
                        receiveWrapper.apply(super::receiveMessage, receiveMessageRequest);
            }

            @Override
            public DeleteMessageBatchResult deleteMessageBatch(DeleteMessageBatchRequest deleteMessageBatchRequest) {
                return deleteBatchWrapper == null ?
                        super.deleteMessageBatch(deleteMessageBatchRequest) :
                        deleteBatchWrapper.apply(super::deleteMessageBatch, deleteMessageBatchRequest);
            }

            @Override
            public void deleteMessage(DeleteMessageRequest deleteMessageRequest) {
                if(deleteWrapper == null) {
                    super.deleteMessage(deleteMessageRequest);
                } else {
                    deleteWrapper.apply((request) -> {
                        super.deleteMessage(request);
                        return null;
                    }, deleteMessageRequest);
                }
            }
        };

        logger.info("AmazonSQSClient client endpoint: {}", configuration.getEndpoint());
        sqsAsync.setEndpoint(configuration.getEndpoint());

        QueueBufferConfig queueBufferConfig = configuration.getQueueBufferConfig().withMaxDoneReceiveBatches(0); //disable messages prefetching
        return new AmazonSQSBufferedAsyncClient(sqsAsync, queueBufferConfig);
    }
}
