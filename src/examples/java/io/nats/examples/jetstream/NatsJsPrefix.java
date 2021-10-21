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
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.examples.ExampleUtils;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static io.nats.examples.jetstream.NatsJsUtils.printStreamInfo;

/**
 * This example will demonstrate connecting on an account that uses a custom prefix.
 *
 * Server configuration (prefix.conf)
 * <pre>
 * port: 4222
 *
 * jetstream: {max_mem_store: 1GB, max_file_store: 1GB}
 *
 * accounts: {
 *   SOURCE: {
 *     jetstream: enabled
 *     users: [ {user: src, password: spass} ]
 *     exports [
 *         { service: "$JS.API.>" },
 *         { service: "sub-made-by-src" },
 *         { service: "sub-made-by-tar" },
 *         { stream: "_INBOX.>" },
 *     ]
 *   },
 *   TARGET: {
 *     jetstream: enabled
 *     users: [ {user: tar, password: tpass} ]
 *     imports [
 *       { service: { account: SOURCE, subject: "$JS.API.>" } , to: tar.api.> }
 *       { service: { account: SOURCE, subject: "sub-made-by-src" }, to: sub-made-by-src}
 *       { service: { account: SOURCE, subject: "sub-made-by-tar" }, to: sub-made-by-tar}
 *       { stream: { account: SOURCE, subject: "_INBOX.>" }, to: "_INBOX.>"}
 *     ]
 *   }
 * }
 * </pre>
 *
 * Start the nats server:
 * <pre>
 * nats-server -c prefix.conf
 * </pre>
 */
public class NatsJsPrefix {
    static final String usageString =
            "\nUsage: java -cp <classpath> NatsJsPrefix"
                    + "\n\nRun Notes:"
                    + "\n   - this example is not configured for arguments so requires manual change"
                    + "\n   - you will need to create the prefix.conf and run the server as in the comments at the top of the code."
                    + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
                    + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
                    + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
                    + "\nUse the URL in the -s server parameter for user/pass/token authentication.\n";

    public static void main(String[] args) {
        String prefix = "tar.api";
        String streamMadeBySrc = "stream-made-by-src";
        String streamMadeByTar = "stream-made-by-tar";
        String subjectMadeBySrc = "sub-made-by.src";
        String subjectMadeByTar = "sub-made-by.tar";
        String serverSrc = "nats://src:spass@localhost:4222";
        String serverTar = "nats://tar:tpass@localhost:4222";

        try (Connection ncSrc = Nats.connect(ExampleUtils.createExampleOptions(serverSrc));
             Connection ncTar = Nats.connect(ExampleUtils.createExampleOptions(serverTar))
        ) {
            // Setup JetStreamOptions. SOURCE does not need prefix
            JetStreamOptions jsoSrc = JetStreamOptions.builder().build();
            JetStreamOptions jsoTar = JetStreamOptions.builder().prefix(prefix).build();

            // Management api allows us to create streams
            JetStreamManagement jsmSrc = ncSrc.jetStreamManagement(jsoSrc);
            JetStreamManagement jsmTar = ncTar.jetStreamManagement(jsoTar);

            // add streams with both account
            System.out.println("\n----------\n1S. Add stream from source account.");
            StreamConfiguration scSrc = StreamConfiguration.builder()
                    .name(streamMadeBySrc)
                    .storageType(StorageType.Memory)
                    .subjects(subjectMadeBySrc)
                    .build();
            printStreamInfo(jsmSrc.addStream(scSrc));

            System.out.println("----------\n1T. Add stream from target account.");
            StreamConfiguration scTar = StreamConfiguration.builder()
                    .name(streamMadeByTar)
                    .storageType(StorageType.Memory)
                    .subjects(subjectMadeByTar)
                    .build();
            printStreamInfo(jsmTar.addStream(scTar));

            // JetStream for both accounts
            System.out.println("----------\n2S. Connect to js from source account.");
            JetStream jsSrc = ncSrc.jetStream(jsoSrc);

            System.out.println("2T. Connect to js from source account.");
            JetStream jsTar = ncTar.jetStream(jsoTar);

            // publish messages from both accounts to both subjects
            System.out.println("----------\n3 S->S. Publish on source stream from source account.");
            jsSrc.publish(subjectMadeBySrc, "src-to-src".getBytes());

            System.out.println("3 S->T. Publish on source target from source account.");
            jsSrc.publish(subjectMadeByTar, "src-to-tar".getBytes());

            System.out.println("3 T->S. Publish on source stream from target account.");
            jsTar.publish(subjectMadeBySrc, "tar-to-src".getBytes());

            System.out.println("3 T->T. Publish on target stream from target account.");
            jsTar.publish(subjectMadeByTar, "tar-to-tar".getBytes());

            // subscribe and read messages
            System.out.println("----------\n4 S<-S. Read from " + subjectMadeBySrc + " from source account");
            readMessages(ncSrc, jsSrc, subjectMadeBySrc);

            System.out.println("4 S<-T. Read from " + subjectMadeByTar + " from source account");
            readMessages(ncSrc, jsSrc, subjectMadeByTar);

            System.out.println("4 T<-S. Read from " + subjectMadeBySrc + " from target account");
            readMessages(ncTar, jsTar, subjectMadeBySrc);

            System.out.println("4 T<-T. Read from " + subjectMadeByTar + " from target account");
            readMessages(ncTar, jsTar, subjectMadeByTar);

            System.out.println("----------\n");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void readMessages(Connection nc, JetStream js, String subject) throws InterruptedException, IOException, JetStreamApiException, TimeoutException {
        JetStreamSubscription sub = js.subscribe(subject);
        nc.flush(Duration.ofSeconds(1));
        Message msg = sub.nextMessage(Duration.ofSeconds(1));
        while (msg != null && msg.isJetStream()) {
            System.out.println("    NatsMessage |" + msg.getSubject() + "|" + new String(msg.getData()));
            msg.ack();
            msg = sub.nextMessage(Duration.ofSeconds(1));
        }
    }
}
