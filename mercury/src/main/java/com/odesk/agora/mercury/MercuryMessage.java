package com.odesk.agora.mercury;

/**
 * Created by Dmitry Solovyov on 11/30/2015.
 */
public class MercuryMessage {
    private final String topicName;
    private final String subject;
    private final String message;

    public MercuryMessage(String topicName, String subject, String message) {
        this.topicName = topicName;
        this.subject = subject;
        this.message = message;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getSubject() {
        return subject;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("TopicName: " + topicName + ",");
        if (subject != null) sb.append(" Subject: " + subject + ",");
        sb.append(" Message: " + message);
        sb.append("}");
        return sb.toString();
    }
}
