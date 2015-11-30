package com.odesk.agora.mercury.consumer;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.util.Topics;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.google.common.base.Strings;
import com.google.inject.AbstractModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by Dmitry Solovyov on 11/23/2015.
 */
public class SQSConsumerModule extends AbstractModule {
    private static final Logger logger = LoggerFactory.getLogger(SQSConsumerModule.class);

    public static final String QUEUE_NAME_DELIMITER = "-";

    private SQSConsumerConfiguration configuration;

    private ScheduledExecutorService consumersExecutor;

    public SQSConsumerModule(SQSConsumerConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    protected void configure() {
        if(configuration.getTopicSubscriptions().isEmpty()) {
            logger.warn("SQSConsumerModule activated, but no topic subscriptions configured.");
            return;
        }

        AmazonSQSClientProvider amazonSQSClientProvider = new AmazonSQSClientProvider(configuration);
        bind(AmazonSQSClient.class).toProvider(amazonSQSClientProvider);

        AmazonSQSClient sqsClient = amazonSQSClientProvider.get();

        AmazonSNSClient snsClient = createSnsClient();

        consumersExecutor = Executors.newScheduledThreadPool(configuration.getConsumerThreadsCorePoolSize());

        TopicMessagesRouter router = new TopicMessagesRouter();
        bind(TopicMessagesRouter.class).toInstance(router);

        for(TopicSubscriptionConfiguration topicConfig : configuration.getTopicSubscriptions()) {
            String topicArn = snsClient.createTopic(topicConfig.getTopicName()).getTopicArn();
            String queueUrl = sqsClient.createQueue(configuration.getQueueNamesPrefix() + QUEUE_NAME_DELIMITER + topicConfig.getTopicName()).getQueueUrl();
            String subscriptionArn = Topics.subscribeQueue(snsClient, sqsClient, topicArn, queueUrl);

            logger.info("SNS topic {} prepared for consuming. TopicArn={}, queueUrl={}, subscriptionArn={}", topicConfig.getTopicName(), topicArn, queueUrl, subscriptionArn);

            consumersExecutor.scheduleWithFixedDelay(new TopicQueueListener(topicConfig, sqsClient, queueUrl, router),
                    topicConfig.getPollingIntervalMs(), topicConfig.getPollingIntervalMs(), TimeUnit.MILLISECONDS);
        }
    }

    private AmazonSNSClient createSnsClient() {
        AmazonSNSClient amazonSNSClient;

        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withConnectionTimeout(configuration.getConnectionTimeout())
                .withSocketTimeout(configuration.getSocketTimeout())
                .withMaxConnections(configuration.getMaxConnections())
                .withMaxErrorRetry(configuration.getMaxErrorRetry());

        final String awsAccessKey = configuration.getAccessKey();
        final String awsSecretKey = configuration.getSecretKey();

        if (Strings.isNullOrEmpty(awsAccessKey) || Strings.isNullOrEmpty(awsSecretKey)) {
            amazonSNSClient = new AmazonSNSClient(new InstanceProfileCredentialsProvider(), clientConfiguration);
        } else {
            BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
            amazonSNSClient = new AmazonSNSClient(basicAWSCredentials, clientConfiguration);
        }

        amazonSNSClient.setEndpoint(configuration.getSNSEndpoint());
        return amazonSNSClient;
    }
}
