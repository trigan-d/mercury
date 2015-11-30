package com.odesk.agora.mercury.publisher;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Dmitry Solovyov on 11/23/2015.
 */
public class SNSPublisherModule extends AbstractModule {
    private static final Logger logger = LoggerFactory.getLogger(SNSPublisherModule.class);

    private SNSPublisherConfiguration configuration;

    public SNSPublisherModule(SNSPublisherConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    protected void configure() {
        AmazonSNSClientProvider amazonSNSClientProvider = new AmazonSNSClientProvider(configuration);
        bind(AmazonSNSClient.class).toProvider(amazonSNSClientProvider);

        AmazonSNSClient snsClient = amazonSNSClientProvider.get();

        for(String topicName : configuration.getTopics()) {
            String topicArn = snsClient.createTopic(topicName).getTopicArn();
            logger.info("SNS topic {} prepared for publishing. TopicArn is {}", topicName, topicArn);
            bind(MercurySNSTopicPublisher.class).annotatedWith(Names.named(topicName)).toInstance(new MercurySNSTopicPublisher(snsClient, topicName, topicArn));
        }
    }
}
