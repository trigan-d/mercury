package com.odesk.agora.mercury.publisher;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.odesk.agora.mercury.MercuryMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Dmitry Solovyov on 11/23/2015.
 */
public class TopicPublisher {
    private final Logger logger;

    private final AmazonSNSClient snsClient;
    private final String topicName;
    private final String topicArn;

    public TopicPublisher(AmazonSNSClient snsClient, String topicName, String topicArn) {
        this.snsClient = snsClient;
        this.topicName = topicName;
        this.topicArn = topicArn;

        logger = LoggerFactory.getLogger(TopicPublisher.class + "-" + topicName);
    }

    public String publish(PublishRequest publishRequest) {
        logger.debug("Sending publishRequest: {}", publishRequest);
        return snsClient.publish(publishRequest.withTopicArn(topicArn)).getMessageId();
    }

    public String publish(String message, String subject) {
        return publish(new PublishRequest(topicArn, message, subject));
    }

    public String publish(MercuryMessage mercuryMessage) {
        return publish(mercuryMessage.getMessage(), mercuryMessage.getSubject());
    }
}