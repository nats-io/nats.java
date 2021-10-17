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
import io.nats.client.api.PurgeResponse;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.examples.ExampleArgs;
import io.nats.examples.ExampleUtils;

import java.util.List;

import static io.nats.examples.jetstream.NatsJsUtils.*;

/**
 * This example will demonstrate JetStream management (admin) api.
 */
public class NatsJsManageStreams {
    static final String usageString =
        "\nUsage: java -cp <classpath> NatsJsManageStreams [-s server] [-strm stream-prefix] [-sub subject-prefix]"
            + "\n\nDefault Values:"
            + "\n   [-strm] manage-stream-"
            + "\n   [-sub] manage-subject-"
            + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
            + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
            + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
            + "\nUse the URL in the -s server parameter for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder("Manage Streams", args, usageString)
            .defaultStream("manage-stream-")
            .defaultSubject("manage-subject-")
            .build();

        String stream1 = exArgs.stream + "1";
        String stream2 = exArgs.stream + "2";
        String subject1 = exArgs.subject + "1";
        String subject2 = exArgs.subject + "2";
        String subject3 = exArgs.subject + "3";
        String subject4 = exArgs.subject + "4";

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server))) {

            // Create a JetStreamManagement context.
            JetStreamManagement jsm = nc.jetStreamManagement();

            // we want to be able to completely create and delete the streams
            // so don't want to work with existing streams
            exitIfStreamExists(jsm, stream1);
            exitIfStreamExists(jsm, stream2);

            // 1. Create (add) a stream with a subject
            System.out.println("\n----------\n1. Configure And Add Stream 1");
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                .name(stream1)
                .subjects(subject1)
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
            //    Thre are very few properties that can actually
            // -  StreamConfiguration is immutable once created
            // -  but the builder can help with that.
            System.out.println("----------\n2. Update Stream 1");
            streamConfig = StreamConfiguration.builder(streamInfo.getConfiguration())
                    .addSubjects(subject2).build();
            streamInfo = jsm.updateStream(streamConfig);
            NatsJsUtils.printStreamInfo(streamInfo);

            // 3. Create (add) another stream with 2 subjects
            System.out.println("----------\n3. Configure And Add Stream 2");
            streamConfig = StreamConfiguration.builder()
                .name(stream2)
                .subjects(subject3, subject4)
                .storageType(StorageType.Memory)
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
            publish(nc, subject1, 5);
            streamInfo = jsm.getStreamInfo(stream1);
            NatsJsUtils.printStreamInfo(streamInfo);

            System.out.println("----------\n4.2 getStreamNames");
            List<String> streamNames = jsm.getStreamNames();
            printObject(streamNames);

            System.out.println("----------\n4.3 getStreams");
            List<StreamInfo> streamInfos = jsm.getStreams();
            NatsJsUtils.printStreamInfoList(streamInfos);

            // 5. Purge a stream of it's messages
            System.out.println("----------\n5. Purge stream");
            PurgeResponse purgeResponse = jsm.purgeStream(stream1);
            printObject(purgeResponse);

            // 6. Delete the streams
            // Subsequent calls to getStreamInfo, deleteStream or purgeStream
            // will throw a JetStreamApiException "stream not found [10059]"
            System.out.println("----------\n6. Delete streams");
            jsm.deleteStream(stream1);
            jsm.deleteStream(stream2);

            // 7. Try to delete the consumer again and get the exception
            System.out.println("----------\n7. Delete stream again");
            try
            {
                jsm.deleteStream(stream1);
            }
            catch (JetStreamApiException e)
            {
                System.out.println("Exception was: '" + e.getMessage() + "'");
            }

            System.out.println("----------\n");
        }
        catch (Exception exp) {
            exp.printStackTrace();
        }
    }
}
