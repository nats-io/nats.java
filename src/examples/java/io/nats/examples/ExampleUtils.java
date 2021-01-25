// Copyright 2018 The NATS Authors
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
import io.nats.client.StreamConfiguration.StorageType;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

public class ExampleUtils {
    public static Options createExampleOptions(String server, boolean allowReconnect) throws Exception {
        Options.Builder builder = new Options.Builder()
                .server(server)
                .connectionTimeout(Duration.ofSeconds(5))
                .pingInterval(Duration.ofSeconds(10))
                .reconnectWait(Duration.ofSeconds(1))
                .errorListener(new ErrorListener() {
                    public void exceptionOccurred(Connection conn, Exception exp) {
                        System.out.println("Exception " + exp.getMessage());
                    }

                    public void errorOccurred(Connection conn, String type) {
                        System.out.println("Error " + type);
                    }

                    public void slowConsumerDetected(Connection conn, Consumer consumer) {
                        System.out.println("Slow consumer");
                    }
                })
                .connectionListener((conn, type) -> System.out.println("Status change "+type));

            if (!allowReconnect) {
                builder = builder.noReconnect();
            } else {
                builder = builder.maxReconnects(-1);
            }

            if (System.getenv("NATS_NKEY") != null && System.getenv("NATS_NKEY") != "") {
                AuthHandler handler = new ExampleAuthHandler(System.getenv("NATS_NKEY"));
                builder.authHandler(handler);
            } else if (System.getenv("NATS_CREDS") != null && System.getenv("NATS_CREDS") != "") {
                builder.authHandler(Nats.credentials(System.getenv("NATS_CREDS")));
            }

        return builder.build();
    }

    public static void createTestStream(JetStream js, String streamName, String subject)
            throws TimeoutException, InterruptedException {
        createTestStream(js, streamName, subject, StorageType.File);
    }

    public static void createTestStream(JetStream js, String streamName, String subject, StorageType storageType)
            throws TimeoutException, InterruptedException {

        // Create a stream, here will use a file storage type, and one subject,
        // the passed subject.
        StreamConfiguration sc = StreamConfiguration.builder()
                .name(streamName)
                .storageType(storageType)
                .subjects(subject)
                .build();
        
        // Add or use an existing stream.
        StreamInfo si = js.addStream(sc);
        System.out.printf("Using stream %s on subject %s created at %s.\n",
           streamName, subject, si.getCreateTime().toLocalTime().toString());
    }    

    // Publish:   [options] <subject> <message>
    // Subscribe: [options] <subject> <msgCount>

    // Request:   [options] <subject> <message>
    // Reply:     [options] <subject> <msgCount>
 
    public static ExampleArgs readPublishArgs(String[] args, String usageString) {
        ExampleArgs ea = new ExampleArgs(args, true, usageString);
        if (ea.message == null) {
            usage(usageString);
        }
        return ea;
    }

    public static ExampleArgs readRequestArgs(String[] args, String usageString) {
        return readPublishArgs(args, usageString);
    }

    public static ExampleArgs readSubscribeArgs(String[] args, String usageString) {
        ExampleArgs ea = new ExampleArgs(args, false, usageString);
        if (ea.msgCount < 1) {
            usage(usageString);
        }
        return ea;
    }

    public static ExampleArgs readReplyArgs(String[] args, String usageString) {
        return readSubscribeArgs(args, usageString);
    }

    public static ExampleArgs readConsumerArgs(String[] args, String usageString) {
        ExampleArgs ea = new ExampleArgs(args, false, usageString);
        if (ea.msgCount < 1 || ea.stream == null || ea.consumer == null) {
            System.out.println("Stream name and consumer name are required to attach.\nSubject and message count are required.\n");
            usage(usageString);
        }
        return ea;
    }

    private static void usage(String usageString) {
        System.out.println(usageString);
        System.exit(-1);
    }
}