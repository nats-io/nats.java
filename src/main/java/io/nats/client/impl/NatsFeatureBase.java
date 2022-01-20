// Copyright 2021 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.client.impl;

import io.nats.client.*;
import io.nats.client.api.*;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static io.nats.client.support.NatsJetStreamConstants.JS_NO_MESSAGE_FOUND_ERR;

public class NatsFeatureBase {

    final NatsJetStream js;
    final JetStreamManagement jsm;

    NatsFeatureBase(NatsConnection connection, FeatureOptions fo) throws IOException {
        if (fo == null) {
            js = new NatsJetStream(connection, null);
            jsm = new NatsJetStreamManagement(connection, null);
        }
        else {
            js = new NatsJetStream(connection, fo.getJetStreamOptions());
            jsm = new NatsJetStreamManagement(connection, fo.getJetStreamOptions());
        }
    }

    MessageInfo _getLastMessage(String streamName, String subject) throws IOException, JetStreamApiException {
        MessageInfo mi = jsm.getLastMessage(streamName, subject);
        if (mi.hasError()) {
            if (mi.getApiErrorCode() == JS_NO_MESSAGE_FOUND_ERR) {
                return null; // run of the mill key not found
            }
            mi.throwOnHasError();
        }
        return mi;
    }

    PublishAck _publish(String subject, Object data, Headers h) throws IOException, JetStreamApiException {
        return js.publish(NatsMessage.builder().subject(subject).data(data).headers(h).build());
    }

    CompletableFuture<PublishAck> _publishAsync(String subject, Object data, Headers h) throws IOException, JetStreamApiException {
        return js.publishAsync(NatsMessage.builder().subject(subject).data(data).headers(h).build());
    }

    interface MessageVisitor {
        void visit(Message msg) throws IOException, JetStreamApiException, InterruptedException;
    }

    void visitSubject(String subject, DeliverPolicy deliverPolicy, boolean headersOnly, boolean ordered, MessageVisitor visitor) throws IOException, JetStreamApiException, InterruptedException {
        PushSubscribeOptions pso = PushSubscribeOptions.builder()
            .ordered(ordered)
            .configuration(
                ConsumerConfiguration.builder()
                    .ackPolicy(AckPolicy.None)
                    .deliverPolicy(deliverPolicy)
                    .headersOnly(headersOnly)
                    .build())
            .build();

        JetStreamSubscription sub = js.subscribe(subject, pso);

        try {
            Duration d100 = Duration.ofMillis(100);
            Message m = sub.nextMessage(Duration.ofMillis(5000)); // give plenty of time for the first
            while (m != null) {
                visitor.visit(m);
                if (m.metaData().pendingCount() == 0) {
                    m = null;
                }
                else {
                    m = sub.nextMessage(d100); // the rest should come pretty quick
                }
            }
        }
        finally {
            sub.unsubscribe();
        }
    }
}
