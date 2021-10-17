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

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;
import io.nats.examples.ExampleArgs;
import io.nats.examples.ExampleUtils;

import java.util.List;

import static io.nats.examples.jetstream.NatsJsUtils.*;

/**
 * This example will demonstrate JetStream management (admin) api stream management.
 */
public class NatsJsManageConsumers {
    static final String usageString =
        "\nUsage: java -cp <classpath> NatsJsManageConsumers [-s server] [-strm stream] [-sub subject] [-dur durable-prefix]"
            + "\n\nDefault Values:"
            + "\n   [-strm] mcon-stream"
            + "\n   [-sub] mcon-subject"
            + "\n   [-dur] mcon-durable-"
            + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
            + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
            + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
            + "\nUse the URL in the -s server parameter for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder("Manage Consumers", args, usageString)
            .defaultStream("mcon-stream")
            .defaultSubject("mcon-subject")
            .defaultDurable("mcon-durable-")
            .build();

        String durable1 = exArgs.durable + "1";
        String durable2 = exArgs.durable + "2";

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server))) {
            // Create a JetStreamManagement context.
            JetStreamManagement jsm = nc.jetStreamManagement();

            // Use the utility to create a stream stored in memory.
            createStreamExitWhenExists(jsm, exArgs.stream, exArgs.subject);

            // 1. Add Consumers
            System.out.println("\n----------\n1. Configure And Add Consumers");
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .durable(durable1) // durable name is required when creating consumers
                    .build();
            ConsumerInfo ci = jsm.addOrUpdateConsumer(exArgs.stream, cc);
            printObject(ci);

            cc = ConsumerConfiguration.builder()
                    .durable(durable2)
                    .build();
            ci = jsm.addOrUpdateConsumer(exArgs.stream, cc);
            printObject(ci);

            // 2. get a list of ConsumerInfo's for all consumers
            System.out.println("\n----------\n2. getConsumers");
            List<ConsumerInfo> consumers = jsm.getConsumers(exArgs.stream);
            printConsumerInfoList(consumers);

            // 3. get a list of all consumers
            System.out.println("\n----------\n3. getConsumerNames");
            List<String> consumerNames = jsm.getConsumerNames(exArgs.stream);
            printObject(consumerNames);

            // 4. Delete a consumer, then list them again
            // Subsequent calls to deleteStream will throw a
            // JetStreamApiException [10014]
            System.out.println("\n----------\n3. Delete consumers");
            jsm.deleteConsumer(exArgs.stream, durable1);
            consumerNames = jsm.getConsumerNames(exArgs.stream);
            printObject(consumerNames);

            // 5. Try to delete the consumer again and get the exception
            System.out.println("\n----------\n5. Delete consumer again");
            try
            {
                jsm.deleteConsumer(exArgs.stream, durable1);
            }
            catch (JetStreamApiException e)
            {
                System.out.println("Exception was: '" + e.getMessage() + "'");
            }

            System.out.println("\n----------");

            // delete the stream since we are done with it.
            jsm.deleteStream(exArgs.stream);
        }
        catch (Exception exp) {
            exp.printStackTrace();
        }
    }
}
