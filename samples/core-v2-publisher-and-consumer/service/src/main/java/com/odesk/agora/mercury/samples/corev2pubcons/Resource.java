package com.odesk.agora.mercury.samples.corev2pubcons;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.odesk.agora.mercury.publisher.TopicPublisher;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * Created by Dmitry Solovyov on 11/30/2015.
 */
@Path("/")
public class Resource extends com.odesk.agora.Resource {
    @Inject @Named("MercuryTestCoreV2")
    private TopicPublisher topicPublisher;

    @GET
    @Path("/publish")
    public void publishMessage(@QueryParam("message") String message, @QueryParam("subject") String subject) {
        topicPublisher.publish(message, subject);
        //for(int i=0;i<9;i++) { topicPublisher.publish(message + i, subject); }
    }
}
