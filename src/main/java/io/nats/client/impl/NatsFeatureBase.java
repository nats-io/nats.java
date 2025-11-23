// Copyright 2022 The NATS Authors
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
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.MessageInfo;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static io.nats.client.support.NatsJetStreamConstants.JS_NO_MESSAGE_FOUND_ERR;

public class NatsFeatureBase {

    protected final NatsJetStream js;
    protected final NatsJetStreamManagement jsm;
    protected String streamName;

    NatsFeatureBase(NatsConnection connection, FeatureOptions fo, NatsJetStreamManagement jsm) throws IOException {
        if (jsm != null) {
            this.jsm = jsm;
            js = (NatsJetStream)jsm.jetStream();
        }
        else if (fo == null) {
            js = new NatsJetStream(connection, null);
            this.jsm = new NatsJetStreamManagement(connection, null);
        }
        else {
            js = new NatsJetStream(connection, fo.getJetStreamOptions());
            this.jsm = new NatsJetStreamManagement(connection, fo.getJetStreamOptions());
        }
    }

    String getStreamName() {
        return streamName;
    }

    protected MessageInfo _getLast(String subject) throws IOException, JetStreamApiException {
        try {
            return jsm.getLastMessage(streamName, subject);
        }
        catch (JetStreamApiException jsae) {
            if (jsae.getApiErrorCode() == JS_NO_MESSAGE_FOUND_ERR) {
                return null;
            }
            throw jsae;
        }
    }

    protected MessageInfo _getBySeq(long seq) throws IOException, JetStreamApiException {
        try {
            return jsm.getMessage(streamName, seq);
        }
        catch (JetStreamApiException jsae) {
            if (jsae.getApiErrorCode() == JS_NO_MESSAGE_FOUND_ERR) {
                return null;
            }
            throw jsae;
        }
    }

    protected void visitSubject(String subject, DeliverPolicy deliverPolicy, boolean headersOnly, boolean ordered, MessageHandler handler) throws IOException, JetStreamApiException, InterruptedException {
        visitSubject(Collections.singletonList(subject), deliverPolicy, headersOnly, ordered, handler);
    }

    protected void visitSubject(List<String> subjects, DeliverPolicy deliverPolicy, boolean headersOnly, boolean ordered, MessageHandler handler) throws IOException, JetStreamApiException, InterruptedException {
        ConsumerConfiguration.Builder ccb = ConsumerConfiguration.builder()
            .ackPolicy(AckPolicy.None)
            .deliverPolicy(deliverPolicy)
            .headersOnly(headersOnly)
            .filterSubjects(subjects);

        PushSubscribeOptions pso = PushSubscribeOptions.builder()
            .stream(streamName)
            .ordered(ordered)
            .configuration(ccb.build())
            .build();

        Duration timeout = js.getTimeout();
        JetStreamSubscription sub = js.subscribe(null, pso);
        try {
            long pending = sub.getConsumerInfo().getCalculatedPending();
            while (pending > 0) { // no need to loop if nothing pending
                Message m = sub.nextMessage(timeout);
                if (m == null) {
                    return; // if there are no messages by the timeout, we are done.
                }
                handler.onMessage(m);
                if (--pending == 0) {
                    return;
                }
            }
        }
        finally {
            sub.unsubscribe();
        }
    }
}
