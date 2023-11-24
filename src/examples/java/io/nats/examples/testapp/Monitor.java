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

package io.nats.examples.testapp;

import io.nats.client.Connection;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.StreamInfo;
import io.nats.examples.testapp.support.CommandLine;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.nats.examples.testapp.Output.formatted;

public class Monitor implements Runnable, java.util.function.Consumer<String> {

    static final String LABEL = "MONITOR";
    static final long REPORT_FREQUENCY = 5000;
    static final int SHORT_REPORTS = 50;

    final CommandLine cmd;
    final Publisher publisher;
    final List<ConnectableConsumer> consumers;
    final AtomicBoolean reportFull;

    public Monitor(CommandLine cmd, Publisher publisher, List<ConnectableConsumer> consumers) {
        this.cmd = cmd;
        this.publisher = publisher;
        this.consumers = consumers;
        reportFull = new AtomicBoolean(true);
    }

    @Override
    public void accept(String s) {
        reportFull.set(true);
        // Output.print(LABEL, s);
    }

    @Override
    public void run() {
        Options options = new Options.Builder()
            .servers(cmd.servers)
            .connectionListener((c, t) -> {
                reportFull.set(true);
                String s = "Connection: " + c.getServerInfo().getPort() + " " + t;
                Output.controlMessage(LABEL, s);
                // Output.print(LABEL, s);
            })
            .errorListener(new OutputErrorListener(LABEL, this) {})
            .maxReconnects(-1)
            .build();

        long started = System.currentTimeMillis();

        int shortReportsOwed = 0;
        try (Connection nc = Nats.connect(options)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            //noinspection InfiniteLoopStatement
            while (true) {
                //noinspection BusyWait
                Thread.sleep(REPORT_FREQUENCY);
                try {
                    StringBuilder conReport = new StringBuilder();
                    if (reportFull.get()) {
                        StreamInfo si = jsm.getStreamInfo(cmd.stream);
                        String message = "Stream\n" + formatted(si.getConfiguration())
                            + "\n" + formatted(si.getClusterInfo());
                        Output.controlMessage(LABEL, message);
                        reportFull.set(false);
                        if (consumers != null) {
                            for (ConnectableConsumer con : consumers) {
                                con.refreshInfo();
                            }
                        }
                    }
                    if (shortReportsOwed < 1) {
                        shortReportsOwed = SHORT_REPORTS;
                        if (consumers != null) {
                            for (ConnectableConsumer con : consumers) {
                                conReport.append("\n").append(con.label).append(" | Last Sequence: ").append(con.getLastReceivedSequence());
                            }
                        }
                    }
                    else {
                        shortReportsOwed--;
                        if (consumers != null) {
                            for (ConnectableConsumer con : consumers) {
                                conReport.append(" | ")
                                    .append(con.name)
                                    .append(": ")
                                    .append(con.getLastReceivedSequence());
                            }
                        }
                    }

                    String pubReport = "";
                    if (publisher != null) {
                        pubReport = "| Publisher: " + publisher.getLastSeqno() +
                            (publisher.isInErrorState() ? " (Paused)" : " (Running)");
                    }
                    Output.controlMessage(LABEL, "Uptime: " + uptime(started) + pubReport + conReport);
                }
                catch (Exception e) {
                    Output.controlMessage(LABEL, e.getMessage());
                    reportFull.set(true);
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static String uptime(long started) {
        return Duration.ofMillis(System.currentTimeMillis() - started).toString().replace("PT", "");
    }
}
