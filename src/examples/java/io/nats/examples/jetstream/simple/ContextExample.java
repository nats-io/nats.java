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

/**
 * This example will demonstrate simplified contexts
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
public class ContextExample {
    private static final String STREAM = "context-stream";
    private static final String SUBJECT = "context-subject";
    private static final String CONSUMER_NAME = "context-consumer";

    public static String SERVER = "nats://localhost:4222";

    public static void main(String[] args) {
        Options options = Options.builder().server(SERVER).build();
        try (Connection nc = Nats.connect(options)) {
            JetStream js = nc.jetStream();
            createOrReplaceStream(nc.jetStreamManagement(), STREAM, SUBJECT);

            // get a stream context from the connection
            StreamContext streamContext = nc.streamContext(STREAM);
            System.out.println("S1. " + streamContext.getStreamInfo());

            // get a stream context from the connection, supplying custom JetStreamOptions
            streamContext = nc.streamContext(STREAM, JetStreamOptions.builder().build());
            System.out.println("S2. " + streamContext.getStreamInfo());

            // get a stream context from the JetStream context
            streamContext = js.streamContext(STREAM);
            System.out.println("S3. " + streamContext.getStreamInfo());

            // when you create a consumer from the stream context you get a ConsumerContext in return
            ConsumerContext consumerContext = streamContext.createOrUpdateConsumer(ConsumerConfiguration.builder().durable(CONSUMER_NAME).build());
            System.out.println("C1. " + consumerContext.getCachedConsumerInfo());

            // get a ConsumerContext from the connection for a pre-existing consumer
            consumerContext = nc.consumerContext(STREAM, CONSUMER_NAME);
            System.out.println("C2. " + consumerContext.getCachedConsumerInfo());

            // get a ConsumerContext from the connection for a pre-existing consumer, supplying custom JetStreamOptions
            consumerContext = nc.consumerContext(STREAM, CONSUMER_NAME, JetStreamOptions.builder().build());
            System.out.println("C3. " + consumerContext.getCachedConsumerInfo());

            // get a ConsumerContext from the stream context for a pre-existing consumer
            consumerContext = streamContext.createConsumerContext(CONSUMER_NAME);
            System.out.println("C4. " + consumerContext.getCachedConsumerInfo());
        }
        catch (JetStreamApiException | IOException | InterruptedException ioe) {
            // JetStreamApiException:
            //      the stream or consumer did not exist
            // IOException:
            //      problem making the connection
            // InterruptedException:
            //      thread interruption in the body of the example
        }
    }
}
