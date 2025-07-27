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
import io.nats.client.api.PublishAck;
import io.nats.client.impl.NatsMessage;
import io.nats.examples.ExampleArgs;
import io.nats.examples.ExampleUtils;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * This example will demonstrate the ability to publish to a stream with either
 * the JetStream.publish(...) or with core Connection.publish(...)
 *
 * The difference lies in the whether it's important to your application to receive
 * a publish ack and whether or not you want to set publish expectations.
 */
public class NatsJsPubVersusCorePub {
    static final String usageString =
        "\nUsage: java -cp <classpath> NatsJsPubVersusCorePub [-s server] [-strm stream] [-sub subject]"
            + "\n\nDefault Values:"
            + "\n   [-strm] js-or-core-stream"
            + "\n   [-sub]  js-or-core-subject"
            + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
            + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
            + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
            + "\nUse the URL in the -s server parameter for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder("Publish JetStream Versus Core", args, usageString)
                .defaultStream("js-or-core-stream")
                .defaultSubject("js-or-core-subject")
                .build();

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server))) {

            // Create a JetStream context.  This hangs off the original connection
            // allowing us to produce data to streams and consume data from
            // JetStream consumers.
            JetStream js = nc.jetStream();

            // Use the utility to create a stream stored in memory.
            NatsJsUtils.createStreamExitWhenExists(nc, exArgs.stream, exArgs.subject);

            // Regular Nats publish is straightforward
            nc.publish(exArgs.subject, "regular-message".getBytes(StandardCharsets.UTF_8));

            // A JetStream publish allows you to set publish options
            // that a regular publish does not.
            // A JetStream publish returns an ack of the publish. There
            // is no ack in a regular message.
            Message msg = NatsMessage.builder()
                    .subject(exArgs.subject)
                    .data("js-message", StandardCharsets.UTF_8)
                    .build();

            PublishAck pa = js.publish(msg);
            System.out.println(pa);

            // set up the subscription
            JetStreamSubscription sub = js.subscribe(exArgs.subject);
            nc.flush(Duration.ofSeconds(5));

            // Both messages appear in the stream
            msg = sub.nextMessage(Duration.ofSeconds(1));
            msg.ack();
            System.out.println("Received Data: " + new String(msg.getData()) + "\n          " + msg.metaData());

            msg = sub.nextMessage(Duration.ofSeconds(1));
            msg.ack();
            System.out.println("Received Data: " + new String(msg.getData()) + "\n          " + msg.metaData());

            // delete the stream since we are done with it.
            nc.jetStreamManagement().deleteStream(exArgs.stream);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
