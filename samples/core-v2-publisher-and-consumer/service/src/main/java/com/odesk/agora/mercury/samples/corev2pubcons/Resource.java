package com.odesk.agora.mercury.samples.corev2pubcons;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.odesk.agora.mercury.publisher.TopicPublisher;
import com.odesk.agora.thrift.hello.THello;

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
    public void publishMessage(@QueryParam("message") String message) {
        //plain text
        //topicPublisher.messageWithTextPayload(message).publish();

        //json and thrift
        topicPublisher.messageWithJsonPayload(new THello("id1", "Hi " + message + " 1", "created_now")).addMetadata("number", "one").publish();
        topicPublisher.messageWithThriftPayload(new THello("id2", "Hi " + message + " 2", "created_later")).addMetadata("number", "two").publish();


        //for(int i=0;i<9;i++) { topicPublisher.messageWithTextPayload(message + i).publish(); }
    }
}
