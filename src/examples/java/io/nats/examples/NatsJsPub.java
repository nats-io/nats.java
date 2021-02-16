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

package io.nats.examples;

import io.nats.client.*;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;

import java.nio.charset.StandardCharsets;

/**
 * This example will demonstrate JetStream publishing.
 *
 * Usage: java NatsJsPub [server]
 *   Use tls:// or opentls:// to require tls, via the Default SSLContext
 *   Set the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.
 *   Set the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.
 *   Use the URL for user/pass/token authentication.
 */
public class NatsJsPub {

    // STREAM, SUBJECT and MESSAGE are required.
    // MSG_COUNT < 1 is the same as 1
    // HEADERS are optional, setup in static initializer if you like
    // HAS_HEADERS will be calculated.
    static final String STREAM = "example-stream";
    static final String SUBJECT = "example-subject";
    static final String MESSAGE = "hello";
    static final int MSG_COUNT = 5;
    static final Headers HEADERS = new Headers();
    static final boolean HAS_HEADERS;

    static {
        // HEADERS.put("key", value);
        HAS_HEADERS = HEADERS.size() > 0;
    }

    public static void main(String[] args) {
        String server = ExampleArgs.getServer(args);

        String hdrNote = HAS_HEADERS ? ", with " + HEADERS.size() + " header(s)" : "";
        System.out.printf("\nPublishing to %s%s. Server is %s\n\n", SUBJECT, hdrNote, server);

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(server))) {

            // Create a JetStream context.  This hangs off the original connection
            // allowing us to produce data to streams and consume data from
            // JetStream consumers.
            JetStream js = nc.jetStream();

            // See NatsJsManagement for examples on how to create the stream
            NatsJsUtils.createOrUpdateStream(nc, STREAM, SUBJECT);

            int stop = MSG_COUNT < 2 ? 2 : MSG_COUNT + 1;
            for (int x = 1; x < stop; x++) {
                // make unique message data if you want more than 1 message
                String data = MSG_COUNT < 2 ? MESSAGE : MESSAGE + "-" + x;

                // create a typical NATS message
                Message msg = NatsMessage.builder()
                        .subject(SUBJECT)
                        .headers(HEADERS)
                        .data(data, StandardCharsets.UTF_8)
                        .build();

                // We'll use the defaults for this simple example, but there are options
                // to constrain publishing to certain streams, expect sequence numbers and
                // more. e.g.:
                //
                // PublishOptions pops = PublishOptions.builder()
                //    .stream("test-stream")
                //    .expectedLastMsgId("transaction-42")
                //    .build();
                // js.publish(msg, pops);

                // Publish a message and print the results of the publish acknowledgement.
                // An exception will be thrown if there is a failure.
                PublishAck pa = js.publish(msg);
                System.out.printf("Published message %s on subject %s, stream %s, seqno %d.\n",
                       data, SUBJECT, pa.getStream(), pa.getSeqno());
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
