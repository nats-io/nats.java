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

package io.nats.examples.testapp;

import io.nats.client.Connection;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.StreamInfo;
import io.nats.client.api.StreamState;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.nats.examples.testapp.Ui.formatted;

public class Monitor implements Runnable {

    static final String ID = "MONITOR";
    static final long REPORT_FREQUENCY = 5000;

    final Publisher publisher;
    final List<ConnectableConsumer> consumers;
    final AtomicBoolean reportFull;

    public Monitor(Publisher publisher, ConnectableConsumer... consumers) {
        this.publisher = publisher;
        this.consumers = Arrays.asList(consumers);
        reportFull = new AtomicBoolean(true);
    }

    @Override
    public void run() {
        Options options = new Options.Builder()
            .server(App.BOOTSTRAP)
            .connectionListener((c, t) -> {
                reportFull.set(true);
                Ui.controlMessage(ID, "Connection: " + c.getServerInfo().getPort() + " " + t);
            })
            .errorListener(new UiErrorListener(ID) {})
            .maxReconnects(-1)
            .build();

        long started = System.currentTimeMillis();

        try (Connection nc = Nats.connect(options)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            //noinspection InfiniteLoopStatement
            while (true) {
                //noinspection BusyWait
                Thread.sleep(REPORT_FREQUENCY);
                try {
                    if (reportFull.get()) {
                        StreamInfo si = jsm.getStreamInfo(App.STREAM);
                        Ui.controlMessage(ID, si.getConfiguration());
                        Ui.controlMessage(ID, formatted(si.getClusterInfo()));
                        reportFull.set(false);
                    }

                    StringBuilder sb = new StringBuilder();
                    for (ConnectableConsumer c : consumers) {
                        sb.append(" | ").append(c.id).append(": ").append(c.getLastReceivedSequence());
                    }
                    Ui.controlMessage(ID,
                        "Uptime: " + Duration.ofMillis(System.currentTimeMillis() - started).toString().replace("PT", "")
                        + " | Published: " + publisher.getLastSeqno()
                        + sb.toString()
                    );
                }
                catch (Exception e) {
                    Ui.controlMessage(ID, e.getMessage());
                    reportFull.set(true);
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private String toString(StreamInfo si) {
        StreamState ss = si.getStreamState();
        return "lastSeq=" + ss.getLastSequence() + ", consumers=" + ss.getConsumerCount();

    }
}
