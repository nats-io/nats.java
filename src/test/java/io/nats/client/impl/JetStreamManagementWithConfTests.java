// Copyright 2020-2025 The NATS Authors
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

package io.nats.client.impl;

import io.nats.client.Connection;
import io.nats.client.JetStreamManagement;
import io.nats.client.NatsTestServer;
import io.nats.client.Options;
import io.nats.client.api.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.nats.client.NatsTestServer.configuredJsServer;
import static io.nats.client.utils.ConnectionUtils.standardConnectionWait;
import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class JetStreamManagementWithConfTests extends JetStreamTestBase {

    @Test
    public void testGetStreamInfoSubjectPagination() throws Exception {
        runInOwnJsServer("pagination.conf", (nc, jsm, js) -> {
            String stream1 = random();
            String stream2 = random();
            long rounds = 101;
            long size = 1000;
            long count = rounds * size;
            jsm.addStream(StreamConfiguration.builder()
                .name(stream1)
                .storageType(StorageType.Memory)
                .subjects("s.*.*")
                .build());

            jsm.addStream(StreamConfiguration.builder()
                .name(stream2)
                .storageType(StorageType.Memory)
                .subjects("t.*.*")
                .build());

            for (int x = 1; x <= rounds; x++) {
                for (int y = 1; y <= size; y++) {
                    js.publish("s." + x + "." + y, null);
                }
            }

            for (int y = 1; y <= size; y++) {
                js.publish("t.7." + y, null);
            }

            StreamInfo si = jsm.getStreamInfo(stream1);
            validateStreamInfo(si.getStreamState(), 0, 0, count);

            si = jsm.getStreamInfo(stream1, StreamInfoOptions.allSubjects());
            validateStreamInfo(si.getStreamState(), count, count, count);

            si = jsm.getStreamInfo(stream1, StreamInfoOptions.filterSubjects("s.7.*"));
            validateStreamInfo(si.getStreamState(), size, size, count);

            si = jsm.getStreamInfo(stream1, StreamInfoOptions.filterSubjects("s.7.1"));
            validateStreamInfo(si.getStreamState(), 1L, 1, count);

            si = jsm.getStreamInfo(stream2, StreamInfoOptions.filterSubjects("t.7.*"));
            validateStreamInfo(si.getStreamState(), size, size, size);

            si = jsm.getStreamInfo(stream2, StreamInfoOptions.filterSubjects("t.7.1"));
            validateStreamInfo(si.getStreamState(), 1L, 1, size);

            List<StreamInfo> infos = jsm.getStreams();
            assertEquals(2, infos.size());
            si = infos.get(0);
            if (si.getConfiguration().getSubjects().get(0).equals("s.*.*")) {
                validateStreamInfo(si.getStreamState(), 0, 0, count);
                validateStreamInfo(infos.get(1).getStreamState(), 0, 0, size);
            }
            else {
                validateStreamInfo(si.getStreamState(), 0, 0, size);
                validateStreamInfo(infos.get(1).getStreamState(), 0, 0, count);
            }

            infos = jsm.getStreams(">");
            assertEquals(2, infos.size());

            infos = jsm.getStreams("*.7.*");
            assertEquals(2, infos.size());

            infos = jsm.getStreams("*.7.1");
            assertEquals(2, infos.size());

            infos = jsm.getStreams("s.7.*");
            assertEquals(1, infos.size());
            assertEquals("s.*.*", infos.get(0).getConfiguration().getSubjects().get(0));

            infos = jsm.getStreams("t.7.1");
            assertEquals(1, infos.size());
            assertEquals("t.*.*", infos.get(0).getConfiguration().getSubjects().get(0));
        });
    }

    private void validateStreamInfo(StreamState streamState, long subjectsList, long filteredCount, long subjectCount) {
        assertEquals(subjectsList, streamState.getSubjects().size());
        assertEquals(filteredCount, streamState.getSubjects().size());
        assertEquals(subjectCount, streamState.getSubjectCount());
    }

    @Test
    public void testAuthCreateUpdateStream() throws Exception {
        try (NatsTestServer ts = configuredJsServer("js_authorization.conf")) {
            Options optionsSrc = optionsBuilder(ts)
                .userInfo("serviceup".toCharArray(), "uppass".toCharArray()).build();

            try (Connection nc = standardConnectionWait(optionsSrc)) {
                JetStreamManagement jsm = nc.jetStreamManagement();

                // add streams with both account
                String stream = random();
                String subject1 = random();
                String subject2 = random();
                StreamConfiguration sc = StreamConfiguration.builder()
                    .name(stream)
                    .storageType(StorageType.Memory)
                    .subjects(subject1)
                    .build();
                StreamInfo si = jsm.addStream(sc);

                sc = StreamConfiguration.builder(si.getConfiguration())
                    .addSubjects(subject2)
                    .build();

                jsm.updateStream(sc);
            }
        }
    }
}
