// Copyright 2020 The NATS Authors
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

package io.nats.examples.jetstream;

import io.nats.client.*;
import io.nats.examples.ExampleArgs;
import io.nats.examples.ExampleUtils;

import java.time.Duration;

import static io.nats.examples.jetstream.NatsJsUtils.createOrUpdateStream;
import static io.nats.examples.jetstream.NatsJsUtils.publish;

/**
 * This example will demonstrate JetStream push subscribing with a delivery subject and how the delivery
 * subject can be used as a subject of a regular Nats message.
 *
 * Usage: java NatsJsPushSubDeliverSubject [-s server] [-strm stream] [-sub subject] [-dlvr deliver] [-mcnt msgCount]
 *   Use tls:// or opentls:// to require tls, via the Default SSLContext
 *   Set the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.
 *   Set the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.
 *   Use the URL for user/pass/token authentication.
 */
public class NatsJsPushSubDeliverSubject {

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder()
                .defaultStream("push-deliver-stream")
                .defaultSubject("push-deliver-actual-subject")
                .defaultDeliver("push-deliver-deliver-subject")
                .defaultMsgCount(0)
                .build(args);

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server, true))) {

            createOrUpdateStream(nc, exArgs.stream, exArgs.subject);

            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // The server uses the delivery subject as both an inbox for a JetStream subscription
            // and as a regular nats messages subject
            PushSubscribeOptions.Builder builder = PushSubscribeOptions.builder();
            builder.deliverSubject(exArgs.deliver);
            PushSubscribeOptions so = builder.build();
            JetStreamSubscription jsSub = js.subscribe(exArgs.subject, so);

            // once the server knows about the delivery subject a regular message subscriber can listen to that
            // notice we subscribe to the delivery subject here
            Subscription regSub = nc.subscribe(exArgs.deliver);

            publish(js, exArgs.subject, 1);

            Message mReg = regSub.nextMessage(Duration.ofSeconds(1));
            System.out.println(mReg.isJetStream() + " | " + mReg);

            Message mJs = jsSub.nextMessage(Duration.ofSeconds(1));
            System.out.println(mJs.isJetStream() + " | " + mJs);

            regSub.unsubscribe();
            nc.flush(Duration.ofSeconds(5));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
