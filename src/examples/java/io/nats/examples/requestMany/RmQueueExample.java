// Copyright 2022 The NATS Authors
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

package io.nats.examples.requestMany;

import io.nats.client.*;
import io.nats.requestMany.RequestMany;
import io.nats.requestMany.RmMessage;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Iterate returns immediately with an unbounded queue, then runs on an executor thread
 */
public class RmQueueExample {
    static final String RESPOND_SUBJECT = "rsvp";
    static final int RESPONDERS = 3;

    public static void main(String[] args) throws Exception {

        Options options = new Options.Builder()
            .server("nats://localhost:4222")
            .errorListener(new ErrorListener() {})
            .build();

        try (Connection nc = Nats.connect(options)) {

            long connTimeout = nc.getOptions().getConnectionTimeout().toMillis();

            // The default request is to get as many in the default time period
            System.out.println("Default Connection Timeout: " + connTimeout + "\n");

            // On an iterate...
            // If there was an exceptional reason for the completion,
            // an RmMessage with a status message or exception
            // and will be the last message added to the queue.
            // If there was no exception reason, a RmMessage.NORMAL_EOD will be added to the queue
            // ...
            // So for iterate, we should poll the queue until there is any End of Data (EOD).

            // We haven't started any responders yet so this will return with a Status 503
            // The good news is that a 503 (or any status or exception)
            // will short circuit and return quickly.
            // We should expect exactly 1 message since we know it's no responders.
            RequestMany rm = RequestMany.builder(nc).build();
            System.out.println("A. Expect 1 EOD Status message. ");
            System.out.println("   " + rm);

            int ix = 0;
            long start = System.currentTimeMillis();
            LinkedBlockingQueue<RmMessage> q = rm.queue(RESPOND_SUBJECT, "rqst".getBytes());
            while (true) {
                RmMessage rmm = q.poll(100, TimeUnit.MILLISECONDS);
                if (rmm != null) {
                    report(ix++, rmm);
                    if (rmm.isEndOfData()) {
                        break;
                    }
                }
            }
            long elapsed = System.currentTimeMillis() - start;
            System.out.println("   Count: " + ix + ", Elapsed: " + elapsed + " ms");

            // start a responder simulator.
            Dispatcher dispatcher = nc.createDispatcher(m -> {
                for (int x = 0; x < RESPONDERS; x++) {
                    nc.publish(m.getReplyTo(), ("R" + x + "-" + new String(m.getData())).getBytes());
                }
            });
            dispatcher.subscribe(RESPOND_SUBJECT);

            // It's okay to reuse the RequestMany object, and
            // since it's connection is its rmm state, it can be used in parallel.
            // The default options contain a stall timeout of 1/10 of the connection timeout.
            // So this should return pretty fast
            rm = RequestMany.builder(nc).build();
            System.out.println("\nB. Expect " + RESPONDERS + " data messages and 1 EOD in slightly more than " + (connTimeout / 10) + " ms.");
            System.out.println("   " + rm);

            // We read until we get an end of data marker
            // This example uses a short polling time so it's possible to get null from the queue
            ix = 0;
            start = System.currentTimeMillis();
            q = rm.queue(RESPOND_SUBJECT, "rqst".getBytes());
            while (true) {
                RmMessage rmm = q.poll(100, TimeUnit.MILLISECONDS);
                if (rmm != null) {
                    report(ix++, rmm);
                    if (rmm.isEndOfData()) {
                        break;
                    }
                }
            }
            elapsed = System.currentTimeMillis() - start;
            System.out.println("   Count: " + ix + ", Elapsed: " + elapsed + " ms");

            // Maybe you always want to wait the full 1 second.
            // Maybe there could be slow (busy?) responders but you still want to hear from them.
            rm = RequestMany.builder(nc).totalWaitTime(1000).build();
            System.out.println("\nC. Expect " + RESPONDERS + " data messages and 1 EOD in slightly more than 1000 ms.");
            System.out.println("   " + rm);

            ix = 0;
            start = System.currentTimeMillis();
            q = rm.queue(RESPOND_SUBJECT, "rqst".getBytes());
            while (true) {
                RmMessage rmm = q.poll(100, TimeUnit.MILLISECONDS);
                if (rmm != null) {
                    report(ix++, rmm);
                    if (rmm.isEndOfData()) {
                        break;
                    }
                }
            }
            elapsed = System.currentTimeMillis() - start;
            System.out.println("   Count: " + ix + ", Elapsed: " + elapsed + " ms");

            // Maybe you just want the first 2. Also limit the time. Slow responders are ignored.
            rm = RequestMany.builder(nc).totalWaitTime(1000).maxResponses(2).build();
            System.out.println("\nD. Expect 2 data messages and 1 EOD very quickly.");
            System.out.println("   " + rm);

            ix = 0;
            start = System.currentTimeMillis();
            q = rm.queue(RESPOND_SUBJECT, "rqst".getBytes());
            while (true) {
                RmMessage rmm = q.poll(100, TimeUnit.MILLISECONDS);
                if (rmm != null) {
                    report(ix++, rmm);
                    if (rmm.isEndOfData()) {
                        break;
                    }
                }
            }
            elapsed = System.currentTimeMillis() - start;
            System.out.println("   Count: " + ix + ", Elapsed: " + elapsed + " ms");
        }
    }

    private static void report(int ix, RmMessage rmm) {
        System.out.println("   " + ix + ". " + rmm);
    }
}
