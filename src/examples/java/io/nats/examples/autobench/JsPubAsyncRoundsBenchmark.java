// Copyright 2015-2018 The NATS Authors
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

package io.nats.examples.autobench;

import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class JsPubAsyncRoundsBenchmark extends AutoBenchmark {

    private final boolean file;
    private final long roundSize;

    public JsPubAsyncRoundsBenchmark(String name, long messageCount, long messageSize, boolean file, long roundSize) {
        super(name, messageCount, messageSize);
        this.file = file;
        this.roundSize = roundSize;
    }

    public void execute(Options connectOptions) throws InterruptedException {
        byte[] payload = createPayload();
        String subject = getSubject();
        String stream = getStream();

        try {
            Connection nc = Nats.connect(connectOptions);

            StreamConfiguration sc = StreamConfiguration.builder()
                    .name(stream)
                    .subjects(subject)
                    .storageType(file ? StorageType.File : StorageType.Memory)
                    .build();
            JetStreamManagement jsm = nc.jetStreamManagement();
            jsm.addStream(sc);

            JetStream js = nc.jetStream();

            try {
                long messagesLeft = getMessageCount();

                this.startTiming();
                while (messagesLeft > 0) {
                    long thisRound;
                    if (messagesLeft > roundSize) {
                        thisRound = roundSize;
                        messagesLeft -= roundSize;
                    }
                    else {
                        thisRound = messagesLeft;
                        messagesLeft = 0;
                    }
                    List<CompletableFuture<PublishAck>> futures = new ArrayList<>((int)roundSize);
                    for (long l = 0; l < thisRound; l++) {
                        futures.add(js.publishAsync(subject, payload));
                    }
                    while (futures.size() > 0) {
                        List<CompletableFuture<PublishAck>> notDone = new ArrayList<>((int)roundSize);
                        for (CompletableFuture<PublishAck> f : futures) {
                            if (!f.isDone()) {
                                notDone.add(f);
                            }
                        }
                        futures = notDone;
                    }
                }
                defaultFlush(nc);
                this.endTiming();
            } finally {
                try {
                    jsm.deleteStream(stream);
                } catch (IOException | JetStreamApiException ex) {
                    this.setException(ex);
                }
                finally {
                    nc.close();
                }
            }
        } catch (IOException | JetStreamApiException ex) {
            this.setException(ex);
        }
    }
}
