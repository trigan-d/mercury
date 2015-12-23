package com.odesk.agora.mercury.consumer;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

/**
 * Created by Dmitry Solovyov on 12/23/2015.
 */
public class ConsumptionRunnable implements Runnable {
    private final TopicQueueListener topicListener;
    private final Message message;

    public ConsumptionRunnable(TopicQueueListener topicListener, Message message) {
        this.topicListener = topicListener;
        this.message = message;
    }

    public String getTopicName() {
        return topicListener.getTopicName();
    }

    @Override
    public void run() {
        String sqsSubject;
        String sqsMessage;

        try {
            JSONObject body = new JSONObject(message.getBody());
            sqsSubject = body.tryGetString("Subject");
            sqsMessage = body.tryGetString("Message");
        } catch (JSONException e) {
            topicListener.getLogger().error("Can't handle SNS message due to json error", e);
            return;
        }

        topicListener.getLogger().debug("Processing Mercury message subject={}, message={}", sqsSubject, sqsMessage);

        try {
            topicListener.processParsedMessage(sqsSubject, sqsMessage);
        } catch (Throwable t) {
            topicListener.getLogger().error("Can't process SQS message", t);
            return;
        }

        topicListener.deleteMessage(message);
    }
}
