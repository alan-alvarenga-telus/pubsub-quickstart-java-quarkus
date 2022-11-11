package com.telusinternational.integration;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class PubSubPublishResource {
    private static final Logger LOG = Logger.getLogger(PubSubPublishResource.class);

    @Inject
    CredentialsProvider credentialsProvider;

    private boolean isSetup = false;

    private Publisher publisher;

    private TopicName topicName;

    /**
     * Create google pubsub path to push the new message
     * @param projectId root project
     * @param topic where to push
     * @throws IOException connectivity error
     */
    private void setup(String projectId, String topic) throws IOException {
        topicName = TopicName.of(projectId, topic);
        publisher = Publisher.newBuilder(topicName)
                .setCredentialsProvider(credentialsProvider)
                .build();
        LOG.infov("Ready to push new message to {0}", topicName.toString());
        isSetup = true;
    }

    /**
     * In push delivery, Pub/Sub initiates requests to your subscriber application to deliver messages.
     * @param projectId root project
     * @param topicId where to push
     * @param data {@code ByteString} message
     * @param callback callback
     * @throws InterruptedException connection error
     */
    public void push(String projectId, String topicId, ByteString data,
                     ApiFutureCallback<String> callback) throws InterruptedException {
        try {
            setup(projectId, topicId);
            if (!isSetup)
                throw new InvalidParameterException("Topic name not defined, use setup fist");
            PubsubMessage message = PubsubMessage.newBuilder().setData(data).build();
            ApiFuture<String> messageIdFuture = publisher.publish(message);
            LOG.infov("publishing message to {0}", topicName.toString());
            ApiFutures.addCallback(messageIdFuture, callback, MoreExecutors.directExecutor());
        }catch (InvalidParameterException | IOException e){
            LOG.info(e.getMessage());
        } finally {
            publisher.shutdown();
            publisher.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

}
