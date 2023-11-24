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
import io.nats.client.JetStream;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.PublishAck;
import io.nats.examples.testapp.support.CommandLine;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class Publisher implements Runnable {

    static final String LABEL = "PUBLISHER";


    final CommandLine cmd;
    final long pubDelay;
    final AtomicLong lastSeqno = new AtomicLong(-1);
    final AtomicLong errorRun = new AtomicLong(0);

    public Publisher(CommandLine cmd, long pubDelay) {
        this.cmd = cmd;
        this.pubDelay = pubDelay;
    }

    public long getLastSeqno() {
        return lastSeqno.get();
    }
    public boolean isInErrorState() {
        return errorRun.get() > 0;
    }

    @Override
    public void run() {
        Options options = new Options.Builder()
            .servers(cmd.servers)
            .connectionListener((c, t) -> Output.controlMessage(LABEL, "Connection: " + c.getServerInfo().getPort() + " " + t))
            .errorListener(new OutputErrorListener(LABEL) {})
            .maxReconnects(-1)
            .build();

        try (Connection nc = Nats.connect(options)) {
            JetStream js = nc.jetStream();
            //noinspection InfiniteLoopStatement
            while (true) {
                if (lastSeqno.get() == -1) {
                    Output.controlMessage(LABEL, "Starting Publish");
                    lastSeqno.set(0);
                }
                try {
                    PublishAck pa = js.publish(cmd.subject, null);
                    lastSeqno.set(pa.getSeqno());
                    if (errorRun.get() > 0) {
                        Output.controlMessage(LABEL, "Restarting Publish");
                    }
                    errorRun.set(0);
                }
                catch (Exception e) {
                    if (errorRun.incrementAndGet() == 1) {
                        Output.controlMessage(LABEL, e.getMessage());
                    }
                }
                try {
                    //noinspection BusyWait
                    Thread.sleep(ThreadLocalRandom.current().nextLong(pubDelay));
                }
                catch (InterruptedException ignore) {}
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
