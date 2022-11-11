package com.telusinternational.integration;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.cloud.pubsub.v1.*;
import com.google.pubsub.v1.*;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.util.Optional;
import java.util.stream.StreamSupport;

@ApplicationScoped
public class PubSubSubscribeResource {
    private static final Logger LOG = Logger.getLogger(PubSubSubscribeResource.class);
    @Inject
    CredentialsProvider credentialsProvider;
    private TopicName topicName;
    private int ackDeadline = 10;

    /**
     * Start listening to messages coming from the specified project-id/subscription-id
     * with {@code exactly-once} subscription type enabled.
     * <p>
     * Pub/Sub supports exactly-once delivery, within a cloud region, based on a Pub/Sub-defined unique message ID.
     * When the feature is enabled, Pub/Sub provides the following guarantees:
     * <ul>
     *     <li>No redelivery occurs once the message has been successfully acknowledged.
     *     <li>No redelivery occurs while a message is outstanding. A message is considered outstanding until the
     *     acknowledgment deadline expires or the message is acknowledged.
     * </ul>
     * <p>
     * In case of multiple valid deliveries, due to acknowledgment deadline expiration or client-initiated negative
     * acknowledgment, only the latest acknowledgment ID can be used to acknowledge the message. Any requests with a
     * previous acknowledgment ID fail.
     *
     * @param projectId      defines the root for the topic and subscription id
     * @param subscriptionId where to listen
     * @param topicId        where to push
     * @param receiver       what to do once received
     * @param deadline       acknowledge deadline for the message
     * @throws IOException connectivity error
     */
    public void subscribeWithExactlyOnce(String projectId, String subscriptionId, String topicId,
                                         MessageReceiver receiver, int deadline) throws IOException {
        ackDeadline = deadline;
        setTopicName(projectId, topicId);
        ProjectSubscriptionName subscriptionName = initSubscription(projectId, subscriptionId, true);
        LOG.infov("Generic subscriber subscribing to {0}-{1}", projectId, subscriptionId);
        listen(receiver, subscriptionName);
    }

    /**
     * By default, Pub/Sub offers at-least-once delivery with no ordering guarantees on all subscription types.
     * <p>
     * Alternatively, if messages have the same ordering key and are in the same region, you can enable message ordering.
     * After you set the message ordering property, the Pub/Sub service delivers messages with the same ordering
     * key and in the order that the Pub/Sub service receives the messages.
     * <p>
     * In general, Pub/Sub delivers each message once and in the order in which it was published.
     * However, messages may sometimes be delivered out of order or more than once.
     * Pub/Sub might redeliver a message even after an acknowledgement request for the message returns successfully.
     *
     * @param projectId      defines the root for the topic and subscription id
     * @param subscriptionId where to listen
     * @param topicId        where to push
     * @param receiver       what to do once received
     * @param deadline       acknowledge deadline for the message
     * @throws IOException connectivity error
     */
    public void subscribeWithAtLeastOnce(String projectId, String subscriptionId, String topicId,
                                         MessageReceiver receiver, int deadline) throws IOException {
        ackDeadline = deadline;
        setTopicName(projectId, topicId);
        ProjectSubscriptionName subscriptionName = initSubscription(projectId, subscriptionId, false);
        LOG.infov("Generic subscriber subscribing to {0}-{1}", projectId, subscriptionId);
        listen(receiver, subscriptionName);
    }

    /**
     * Let the stream begin!
     *
     * @param receiver         what to do once received
     * @param subscriptionName subscription path
     */
    private void listen(MessageReceiver receiver, ProjectSubscriptionName subscriptionName) {
        Subscriber subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
        subscriber.startAsync().awaitRunning();
    }

    /**
     * Set the topic name in order to build a proper subscriber.
     *
     * @param projectId defines the root for the topic and subscription id
     * @param topicId   where to push
     */
    private void setTopicName(String projectId, String topicId) throws IOException {
        // List all existing subscriptions and create the 'subscriptionId' if needed
        topicName = TopicName.of(projectId, topicId);
        boolean topicExists = false;
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
            ProjectName projectName = ProjectName.of(projectId);
            // does topic exists?
            for (Topic _topic : topicAdminClient.listTopics(projectName).iterateAll()) {
                if (_topic.getName().equals(topicName.toString())) {
                    topicExists = true;
                    LOG.infov("Topic {0} already exists.", topicName.toString());
                }
            }
            if (!topicExists) {
                Topic topic =
                        topicAdminClient.createTopic(
                                Topic.newBuilder()
                                        .setName(topicName.toString())
                                        .build());

                LOG.infov("Created topic with schema: {0}", topic.getName());
            }
        } catch (AlreadyExistsException e) {
            LOG.infov("Topic {0} already exists.", topicName.toString());
        }

    }

    /**
     * Create a new subscription dependent on the selected subscription-id
     *
     * @param projectId      defines the root for the topic and subscription id
     * @param subscriptionId where to listen
     * @param once           enable exactly once method
     * @return none
     * @throws IOException connectivity error
     */
    private ProjectSubscriptionName initSubscription(String projectId, String subscriptionId,
                                                     boolean once) throws IOException {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);
        SubscriptionAdminSettings subscriptionAdminSettings = SubscriptionAdminSettings.newBuilder()
                .setCredentialsProvider(credentialsProvider)
                .build();
        try (SubscriptionAdminClient subscriptionAdminClient =
                     SubscriptionAdminClient.create(subscriptionAdminSettings)) {
            Iterable<Subscription> subscriptions =
                    subscriptionAdminClient.listSubscriptions(ProjectName.of(projectId))
                            .iterateAll();
            Optional<Subscription> existing =
                    StreamSupport.stream(subscriptions.spliterator(), false)
                            .filter(sub -> sub.getName().equals(subscriptionName.toString()))
                            .findFirst();
            if (!existing.isPresent()) {
                // subscribe with exactly once delivery mode enabled.
                subscriptionAdminClient.createSubscription(Subscription.newBuilder()
                        .setName(subscriptionName.toString())
                        .setTopic(topicName.toString())
                        .setEnableExactlyOnceDelivery(once)
                        .setPushConfig(PushConfig.getDefaultInstance())
                        .setAckDeadlineSeconds(ackDeadline)
                        .build());
            }
        }
        return subscriptionName;
    }

}
