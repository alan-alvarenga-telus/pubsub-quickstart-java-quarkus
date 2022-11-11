package com.telusinternational;


import com.google.api.core.ApiFutureCallback;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.protobuf.ByteString;
import com.telusinternational.integration.PubSubPublishResource;
import com.telusinternational.integration.PubSubSubscribeResource;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.inject.Inject;

import static io.quarkus.arc.ComponentsProvider.LOG;

@QuarkusMain
public class MainResource implements QuarkusApplication {
    @Inject
    PubSubSubscribeResource subscribeResource;

    @Inject
    PubSubPublishResource publishResource;

    @ConfigProperty(name = "quarkus.google.cloud.project-id")
    String projectId;

    @ConfigProperty(name = "quarkus.google.cloud.subscription-id")
    String subscriptionId;

    @ConfigProperty(name = "quarkus.google.cloud.topic-id")
    String topicId;


    @Override
    public int run(String... args) throws Exception {
        ByteString data = ByteString.copyFromUtf8("my-message");
        MessageReceiver receiver = (message, consumer) -> {
            LOG.infov("Got message id {0}", message.getData().toStringUtf8());
            consumer.ack();
        };
        subscribeResource.subscribeWithExactlyOnce(projectId, subscriptionId, topicId, receiver, 20);
        publishResource.push(projectId, topicId, data, new ApiFutureCallback<String>() {
            @Override
            public void onFailure(Throwable throwable) {
                System.out.println("fail");
            }

            @Override
            public void onSuccess(String s) {
                System.out.println("success" + s);
            }
        });

        return 0;
    }
}
