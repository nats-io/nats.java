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
import io.nats.client.JetStream;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.PublishAck;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class Publisher implements Runnable {

    static final String ID = "PUBLISHER";

    long pubDelay;
    AtomicLong lastSeqno = new AtomicLong(-1);
    int errorRun = 0;

    public Publisher(long pubDelay) {
        this.pubDelay = pubDelay;
    }

    public long getLastSeqno() {
        return lastSeqno.get();
    }

    @Override
    public void run() {
        Options options = new Options.Builder()
            .server(App.BOOTSTRAP)
            .connectionListener((c, t) -> Ui.controlMessage(ID, "Connection: " + c.getServerInfo().getPort() + " " + t))
            .errorListener(new UiErrorListener(ID) {})
            .maxReconnects(-1)
            .build();

        try (Connection nc = Nats.connect(options)) {
            JetStream js = nc.jetStream();
            //noinspection InfiniteLoopStatement
            while (true) {
                if (lastSeqno.get() == -1) {
                    Ui.controlMessage(ID, "Starting Publish");
                    lastSeqno.set(0);
                }
                try {
                    PublishAck pa = js.publish(App.SUBJECT, null);
                    lastSeqno.set(pa.getSeqno());
                    if (errorRun > 0) {
                        Ui.controlMessage(ID, "Restarting Publish");
                    }
                    errorRun = 0;
                }
                catch (Exception e) {
                    if (++errorRun == 1) {
                        Ui.controlMessage(ID, e.getMessage());
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
