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
import io.nats.examples.ExampleArgs;
import io.nats.examples.ExampleUtils;

/**
 * This example will demonstrate JetStream publishing with options.
 */
public class NatsJsPubWithOptionsUseCases {
    static final String usageString =
            "\nUsage: java -cp <classpath> NatsJsPubWithOptionsUseCases [-s server] [-strm stream] [-sub subject]"
                    + "\n\nDefault Values:"
                    + "\n   [-strm stream]     pubopts-stream"
                    + "\n   [-sub subject]     pubopts-subject"
                    + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
                    + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
                    + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
                    + "\nUse the URL for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder()
                .defaultStream("pubopts-stream")
                .defaultSubject("pubopts-subject")
                .build(args, usageString);

        String hdrNote = exArgs.hasHeaders() ? ", with " + exArgs.headers.size() + " header(s)" : "";
        System.out.printf("\nPublishing to %s%s. Server is %s\n\n", exArgs.subject, hdrNote, exArgs.server);

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server))) {

            // Create a JetStream context.  This hangs off the original connection
            // allowing us to produce data to streams and consume data from
            // JetStream consumers.
            JetStream js = nc.jetStream();

            // See NatsJsManagement for examples on how to create the stream
            NatsJsUtils.createOrUpdateStream(nc, exArgs.stream, exArgs.subject);

            PublishOptions.Builder pubOptsBuilder = PublishOptions.builder()
                    .expectedStream("pubopts-stream")
                    .messageId("mid1");
            PublishAck pa = js.publish(exArgs.subject, "message1".getBytes(), pubOptsBuilder.build());
            System.out.printf("Published message on subject %s, stream %s, seqno %d.\n",
                    exArgs.subject, pa.getStream(), pa.getSeqno());

            // IMPORTANT!
            // You can reuse the builder in 2 ways.
            // 1. Manually set a field to null or to UNSET_LAST_SEQUENCE if you want to clear it out.
            // 2. Use the clearExpected method to clear the expectedLastId, expectedLastSequence and messageId fields

            // Manual re-use 1. Clearing some fields
            pubOptsBuilder
                    .expectedLastMsgId("mid1")
                    .expectedLastSequence(PublishOptions.UNSET_LAST_SEQUENCE)
                    .messageId(null);
            pa = js.publish(exArgs.subject, "message2".getBytes(), pubOptsBuilder.build());
            System.out.printf("Published message on subject %s, stream %s, seqno %d.\n",
                    exArgs.subject, pa.getStream(), pa.getSeqno());

            // Manual re-use 2. Setting all the expected fields again
            pubOptsBuilder
                    .expectedLastMsgId(null)
                    .expectedLastSequence(pa.getSeqno()) // last sequence can be found in last ack
                    .messageId("mid3");
            pa = js.publish(exArgs.subject, "message3".getBytes(), pubOptsBuilder.build());
            System.out.printf("Published message on subject %s, stream %s, seqno %d.\n",
                    exArgs.subject, pa.getStream(), pa.getSeqno());

            // reuse() method clears all the fields, then we set some fields.
            pubOptsBuilder.clearExpected()
                    .expectedLastSequence(pa.getSeqno()) // last sequence can be found in last ack
                    .messageId("mid4");
            pa = js.publish(exArgs.subject, "message4".getBytes(), pubOptsBuilder.build());
            System.out.printf("Published message on subject %s, stream %s, seqno %d.\n",
                    exArgs.subject, pa.getStream(), pa.getSeqno());

            // exception when the expected stream does not match
            try {
                PublishOptions opts = PublishOptions.builder().expectedStream("wrongStream").build();
                js.publish(exArgs.subject, "ex1".getBytes(), opts);
            } catch (JetStreamApiException e) {
                System.out.println(e);
            }

            // exception with wrong last msg ID
            try {
                PublishOptions opts = PublishOptions.builder().expectedLastMsgId("wrongId").build();
                js.publish(exArgs.subject, "ex2".getBytes(), opts);
            } catch (JetStreamApiException e) {
                System.out.println(e);
            }

            // exception with wrong last sequence
            try {
                PublishOptions opts = PublishOptions.builder().expectedLastSequence(999).build();
                js.publish(exArgs.subject, "ex3".getBytes(), opts);
            } catch (JetStreamApiException e) {
                System.out.println(e);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
