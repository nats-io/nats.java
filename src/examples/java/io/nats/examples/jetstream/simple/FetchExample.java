// Copyright 2023 The NATS Authors
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

package io.nats.examples.jetstream.simple;

import io.nats.client.*;

import java.io.IOException;

import static io.nats.examples.jetstream.simple.Utils.*;

/**
 * This example will demonstrate simplified fetch
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
public class FetchExample {
    private static final String STREAM = "fetch-stream";
    private static final String SUBJECT = "fetch-subject";
    private static final String MESSAGE_TEXT = "fetch";
    private static final String CONSUMER_NAME_PREFIX = "fetch-consumer";
    private static final int MESSAGES = 20;
    private static final int EXPIRES_SECONDS = 2;

    public static String SERVER = "nats://localhost:4222";

    public static void main(String[] args) {
        Options options = Options.builder().server(SERVER).build();
        try (Connection nc = Nats.connect(options)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            // set's up the stream and publish data
            createOrReplaceStream(jsm, STREAM, SUBJECT);
            setupPublish(js, SUBJECT, MESSAGE_TEXT, MESSAGES);

            // 1. Different fetch max messages demonstrate expiration behavior

            // 1A. equal number of messages to the fetch max messages
            simpleFetch(jsm, js, "1A", 20, 0);

            // 1B. more messages than the fetch max messages
            simpleFetch(jsm, js, "1B", 10, 0);

            // 1C. fewer messages than the fetch max messages
            simpleFetch(jsm, js, "1C", 40, 0);

            // 1D. "fetch-consumer-40-messages" was created in 1C and has no messages available
            simpleFetch(jsm, js, "1D", 40, 0);

            // bytes don't work before server v2.9.1
            if (nc.getServerInfo().isOlderThanVersion("2.9.1")) {
                return;
            }

            // 2. Different max bytes sizes demonstrate expiration behavior
            //    - each test message is approximately 100 bytes

            // 2A. max bytes is reached before message count
            simpleFetch(jsm, js, "2A", 0, 700);

            // 2B. fetch max messages is reached before byte count
            simpleFetch(jsm, js, "2B", 10, 1500);

            // 2C. fewer bytes than the byte count
            simpleFetch(jsm, js, "2C", 0, 3000);
        }
        catch (IOException ioe) {
            // problem making the connection or
        }
        catch (InterruptedException e) {
            // thread interruption in the body of the example
        }
    }

    private static void simpleFetch(JetStreamManagement jsm, JetStream js, String label, int maxMessages, int maxBytes) {
        String consumerName = generateConsumerName(maxMessages, maxBytes);

        // Pre define a consumer
        createConsumer(jsm, STREAM, consumerName);

        // Create the Consumer Context
        ConsumerContext consumerContext;
        try {
            consumerContext = js.getConsumerContext(STREAM, consumerName);
        }
        catch (IOException e) {
            return; // likely a connection problem
        }
        catch (JetStreamApiException e) {
            return; // the stream or consumer did not exist
        }

        // Custom FetchConsumeOptions
        FetchConsumeOptions.Builder builder = FetchConsumeOptions.builder().expiresIn(EXPIRES_SECONDS * 1000);
        if (maxMessages == 0) {
            builder.maxBytes(maxBytes);
        }
        else if (maxBytes == 0) {
            builder.maxMessages(maxMessages);
        }
        else {
            builder.maxBytes(maxBytes, maxMessages);
        }
        FetchConsumeOptions fetchConsumeOptions = builder.build();

        printExplanation(label, consumerName, maxMessages, maxBytes);

        long start = System.currentTimeMillis();

        // create the consumer then use it
        FetchConsumer consumer = consumerContext.fetch(fetchConsumeOptions);
        int received = 0;
        try {
            Message msg = consumer.nextMessage();
            while (msg != null) {
                ++received;
                msg.ack();
                msg = consumer.nextMessage();
            }
        }
        catch (InterruptedException e) {
            // this should never happen unless the
            // developer interrupts this thread
            System.err.println("Treating InterruptedException as fatal error.");
            System.exit(-1);
        }
        catch (JetStreamStatusCheckedException e) {
            // either the consumer was deleted in the middle
            // of the pull or there is a new status from the
            // server that this client is not aware of
            System.err.println("Treating JetStreamStatusCheckedException as fatal error.");
            System.exit(-1);
        }
        long elapsed = System.currentTimeMillis() - start;

        printSummary(received, elapsed);
    }

    private static String generateConsumerName(int maxMessages, int maxBytes) {
        if (maxBytes == 0) {
            return CONSUMER_NAME_PREFIX + "-" + maxMessages + "-messages";
        }
        if (maxMessages == 0) {
            return CONSUMER_NAME_PREFIX + "-" + maxBytes + "-bytes-unlimited-messages";
        }
        return CONSUMER_NAME_PREFIX + "-" + maxBytes + "-bytes-" + maxMessages + "-messages";
    }

    private static void printSummary(int received, long elapsed) {
        System.out.println("+++ " + received + " message(s) were received in " + elapsed + "ms\n");
    }

    private static void printExplanation(String label, String name, int maxMessages, int maxBytes) {
        System.out.println("--------------------------------------------------------------------------------");
        System.out.println(label + ". " + name);
        switch (label) {
            case "1A":
            case "1B":
                System.out.println("=== Fetch (" + maxMessages + ") is less than or equal to available messages (" + MESSAGES + ")");
                System.out.println("=== nextMessage() will return null when consume is done");
                break;
            case "1C":
                System.out.println("=== Fetch (" + maxMessages + ") is larger than available messages (" + MESSAGES + ")");
                System.out.println("=== FetchConsumeOption \"expires in\" is " + EXPIRES_SECONDS + " seconds.");
                System.out.println("=== nextMessage() blocks until expiration when there are no messages available, then returns null.");
                break;
            case "1D":
                System.out.println("=== Fetch (" + maxMessages + ") is larger than available messages (0)");
                System.out.println("=== FetchConsumeOption \"expires in\" is " + EXPIRES_SECONDS + " seconds.");
                System.out.println("=== nextMessage() blocks until expiration when there are no messages available, then returns null.");
                break;
            case "2A":
                System.out.println("=== Max bytes (" + maxBytes + ") will be reached.");
                System.out.println("=== nextMessage() will return null when consume is done");
                break;
            case "2B":
                System.out.println("=== Fetch max messages (" + maxMessages + ") will be reached before max bytes (" + maxBytes + ")");
                System.out.println("=== nextMessage() will return null when consume is done");
                break;
            case "2C":
                System.out.println("=== Max bytes (" + maxBytes + ") is larger than available bytes (approximately 2000).");
                System.out.println("=== FetchConsumeOption \"expires in\" is " + EXPIRES_SECONDS + " seconds.");
                System.out.println("=== nextMessage() blocks until expiration when there are no messages available, then returns null.");
                break;
        }
    }
}
