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
import io.nats.client.api.ConsumerConfiguration;

import java.io.IOException;

import static io.nats.examples.jetstream.simple.Utils.createOrReplaceStream;
import static io.nats.examples.jetstream.simple.Utils.publish;

/**
 * This example will demonstrate simplified fetch
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
public class FetchMessagesExample {
    private static final String STREAM = "fetch-messages-stream";
    private static final String SUBJECT = "fetch-messages-subject";
    private static final String MESSAGE_TEXT = "fetch-messages";
    private static final String CONSUMER_NAME_PREFIX = "fetch-messages-consumer";
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
            publish(js, SUBJECT, MESSAGE_TEXT, MESSAGES);

            // Different fetch max messages demonstrate expiration behavior

            // A. equal number of messages to the fetch max messages
            simpleFetch(nc, js, "A", 20);

            // B. more messages than the fetch max messages
            simpleFetch(nc, js, "B", 10);

            // C. fewer messages than the fetch max messages
            simpleFetch(nc, js, "C", 40);

            // D. "fetch-consumer-40-messages" was created in 1C and has no messages available
            simpleFetch(nc, js, "D", 40);
        }
        catch (IOException ioe) {
            // problem making the connection or
        }
        catch (InterruptedException e) {
            // thread interruption in the body of the example
        }
    }

    private static void simpleFetch(Connection nc, JetStream js, String label, int maxMessages) {
        String consumerName = CONSUMER_NAME_PREFIX + "-" + maxMessages + "-messages";

        // get stream context, create consumer and get the consumer context
        StreamContext streamContext;
        ConsumerContext consumerContext;
        try {
            streamContext = nc.streamContext(STREAM);
            consumerContext = streamContext.createOrUpdateConsumer(ConsumerConfiguration.builder().durable(consumerName).build());
        }
        catch (JetStreamApiException | IOException e) {
            // JetStreamApiException:
            //      the stream or consumer did not exist
            // IOException:
            //      likely a connection problem
            return;
        }

        // Custom FetchConsumeOptions
        FetchConsumeOptions fetchConsumeOptions = FetchConsumeOptions.builder()
            .maxMessages(maxMessages)
            .expiresIn(EXPIRES_SECONDS * 1000)
            .build();

        printExplanation(label, consumerName, maxMessages);

        // create the consumer then use it
        int receivedMessages = 0;
        long start = System.currentTimeMillis();
        try (FetchConsumer consumer = consumerContext.fetch(fetchConsumeOptions)) {
            Message msg = consumer.nextMessage();
            while (msg != null) {
                msg.ack();
                if (++receivedMessages == maxMessages) {
                    msg = null;
                }
                else {
                    msg = consumer.nextMessage();
                }
            }
        }
        catch (JetStreamApiException | JetStreamStatusCheckedException | IOException | InterruptedException e) {
            // JetStreamApiException:
            //      api calls under the covers theoretically this could fail, but practically it won't.
            // JetStreamStatusCheckedException:
            //      Either the consumer was deleted in the middle
            //      of the pull or there is a new status from the
            //      server that this client is not aware of
            // IOException:
            //      likely a connection problem
            // InterruptedException:
            //      developer interrupted this thread?
            System.err.println("Exception should be handled properly, just exiting here.");
            System.exit(-1);
        }
        catch (Exception e) {
            // this is from the FetchConsumer being AutoCloseable, but should never be called
            // as work inside the close is already guarded by try/catch
            System.err.println("Exception should be handled properly, just exiting here.");
            System.exit(-1);
        }
        long elapsed = System.currentTimeMillis() - start;

        printSummary(receivedMessages, elapsed);
    }

    private static void printSummary(int received, long elapsed) {
        System.out.println("+++ Fetch executed and " + received + " message(s) were received in " + elapsed + "ms\n");
    }

    private static void printExplanation(String label, String name, int maxMessages) {
        System.out.println("--------------------------------------------------------------------------------");
        System.out.println(label + ". " + name);
        switch (label) {
            case "A":
            case "B":
                System.out.println("=== Fetch (" + maxMessages + ") is less than or equal to available messages (" + MESSAGES + ")");
                System.out.println("=== nextMessage() will return null when consume is done");
                break;
            case "C":
                System.out.println("=== Fetch (" + maxMessages + ") is larger than available messages (" + MESSAGES + ")");
                System.out.println("=== FetchConsumeOption \"expires in\" is " + EXPIRES_SECONDS + " seconds.");
                System.out.println("=== nextMessage() blocks until expiration when there are no messages available, then returns null.");
                break;
            case "D":
                System.out.println("=== Fetch (" + maxMessages + ") is larger than available messages (0)");
                System.out.println("=== FetchConsumeOption \"expires in\" is " + EXPIRES_SECONDS + " seconds.");
                System.out.println("=== nextMessage() blocks until expiration when there are no messages available, then returns null.");
                break;
        }
    }
}
