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
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.PurgeResponse;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.examples.ExampleArgs;
import io.nats.examples.ExampleUtils;

import java.util.List;

import static io.nats.examples.jetstream.NatsJsUtils.printObject;
import static io.nats.examples.jetstream.NatsJsUtils.publish;

/**
 * This example will demonstrate JetStream management (admin) api.
 */
public class NatsJsManageStreams {
    static final String usageString =
            "\nUsage: java -cp <classpath> NatsJsManageStreams [-s server]"
                    + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
                    + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
                    + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
                    + "\nUse the URL for user/pass/token authentication.\n";

    private static final String STREAM1 = "manage-stream1";
    private static final String STREAM2 = "manage-stream2";
    private static final String SUBJECT1 = "manage-subject1";
    private static final String SUBJECT2 = "manage-subject2";
    private static final String SUBJECT3 = "manage-subject3";
    private static final String SUBJECT4 = "manage-subject4";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleUtils.optionalServer(args, usageString);

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server))) {

            // Create a JetStreamManagement context.
            JetStreamManagement jsm = nc.jetStreamManagement();

            // 1. Create (add) a stream with a subject
            // -  Full configuration schema:
            //    https://github.com/nats-io/jsm.go/blob/main/schemas/jetstream/api/v1/stream_configuration.json
            System.out.println("\n----------\n1. Configure And Add Stream 1");
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .name(STREAM1)
                    .subjects(SUBJECT1)
                    // .retentionPolicy(...)
                    // .maxConsumers(...)
                    // .maxBytes(...)
                    // .maxAge(...)
                    // .maxMsgSize(...)
                    .storageType(StorageType.Memory)
                    // .replicas(...)
                    // .noAck(...)
                    // .template(...)
                    // .discardPolicy(...)
                    .build();
            StreamInfo streamInfo = jsm.addStream(streamConfig);
            NatsJsUtils.printStreamInfo(streamInfo);

            // 2. Update stream, in this case add a subject
            // -  StreamConfiguration is immutable once created
            // -  but the builder can help with that.
            System.out.println("----------\n2. Update Stream 1");
            streamConfig = StreamConfiguration.builder(streamInfo.getConfiguration())
                    .addSubjects(SUBJECT2).build();
            streamInfo = jsm.updateStream(streamConfig);
            NatsJsUtils.printStreamInfo(streamInfo);

            // 3. Create (add) another stream with 2 subjects
            System.out.println("----------\n3. Configure And Add Stream 2");
            streamConfig = StreamConfiguration.builder()
                    .name(STREAM2)
                    .storageType(StorageType.Memory)
                    .subjects(SUBJECT3, SUBJECT4)
                    .build();
            streamInfo = jsm.addStream(streamConfig);
            NatsJsUtils.printStreamInfo(streamInfo);

            // 4. Get information on streams
            // 4.0 publish some message for more interesting stream state information
            // -   SUBJECT1 is associated with STREAM1
            // 4.1 getStreamInfo on a specific stream
            // 4.2 get a list of all streams
            // 4.3 get a list of StreamInfo's for all streams
            System.out.println("----------\n4.1 getStreamInfo");
            publish(nc, SUBJECT1, 5);
            streamInfo = jsm.getStreamInfo(STREAM1);
            NatsJsUtils.printStreamInfo(streamInfo);

            System.out.println("----------\n4.2 getStreamNames");
            List<String> streamNames = jsm.getStreamNames();
            printObject(streamNames);

            System.out.println("----------\n4.2 getStreamNames");
            List<StreamInfo> streamInfos = jsm.getStreams();
            NatsJsUtils.printStreamInfoList(streamInfos);

            // 5. Purge a stream of it's messages
            System.out.println("----------\n5. Purge stream");
            PurgeResponse purgeResponse = jsm.purgeStream(STREAM1);
            printObject(purgeResponse);

            // 6. Delete a stream
            // Subsequent calls to getStreamInfo, deleteStream or purgeStream
            // will throw a JetStreamApiException "stream not found (404)"
            System.out.println("----------\n6. Delete stream");
            jsm.deleteStream(STREAM2);

            System.out.println("----------\n");
        }
        catch (Exception exp) {
            exp.printStackTrace();
        }
    }
}
