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

import static io.nats.examples.jetstream.NatsJsUtils.createOrUpdateStream;

/**
 * This example will demonstrate the ability to publish to a stream with either
 * the JetStream.publish(...) or with regular Connection.publish(...)
 *
 * The difference lies in the whether it's important to your application to receive
 * a publish ack and whether or not you want to set publish expectations.
 */
public class NatsJsPubVsRegularPub {
    static final String usageString =
            "\nUsage: java -cp <classpath> NatsJsPubVsRegularPub [-s server] [-strm stream] [-sub subject]"
                    + "\n\nDefault Values:"
                    + "\n   [-strm stream]     regular-stream"
                    + "\n   [-sub subject]     regular-subject"
                    + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
                    + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
                    + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
                    + "\nUse the URL for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder()
                .defaultStream("regular-stream")
                .defaultSubject("regular-subject")
                .build(args, usageString);

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server))) {
            JetStream js = nc.jetStream();
            createOrUpdateStream(nc, exArgs.stream, exArgs.subject);

            JetStreamSubscription sub = js.subscribe(exArgs.subject);
            nc.flush(Duration.ofSeconds(5));

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

            PublishOptions po = PublishOptions.builder()
//                    .expectedLastSeqence(...)
//                    .expectedLastMsgId(...)
//                    .expectedStream(...)
//                    .messageId(...)
                    .build();

            PublishAck pa = js.publish(msg, po);
            System.out.println(pa);

            // But both messages appear in the stream
            msg = sub.nextMessage(Duration.ofSeconds(1));
            System.out.println("Received: " + new String(msg.getData()));

            msg = sub.nextMessage(Duration.ofSeconds(1));
            System.out.println("Received: " + new String(msg.getData()));

        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
