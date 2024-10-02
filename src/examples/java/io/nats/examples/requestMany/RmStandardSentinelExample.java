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

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Standard sentinel expect a null or empty payload to indicate that there will be no more messages.
 * <p>Standard sentinel works with the RequestMany.fetch, RequestMany.iterate and RequestMany.request,
 * although you probably would not use it with request as you can provide your own sentinel handling.
 */
public class RmStandardSentinelExample {
    static final String RESPOND_SUBJECT = "rsvp";
    static final int MESSAGES_BEFORE_SENTINEL = 3;

    public static void main(String[] args) throws Exception {

        Options options = new Options.Builder()
            .server("nats://localhost:4222")
            .errorListener(new ErrorListener() {})
            .build();

        try (Connection nc = Nats.connect(options)) {

            // Use the standard sentinel builder shortcut
            RequestMany rm = RequestMany.standardSentinel(nc);
            System.out.println(rm);

            // start a responder simulator.
            Dispatcher dispatcher = nc.createDispatcher(m -> {
                for (int x = 0; x < MESSAGES_BEFORE_SENTINEL; x++) {
                    nc.publish(m.getReplyTo(), ("R" + x + "-" + new String(m.getData())).getBytes());
                }
                nc.publish(m.getReplyTo(), null);
            });
            dispatcher.subscribe(RESPOND_SUBJECT);

            // Standard Sentinel can work with RequestMany.fetch
            System.out.println("\nFetch, Expect " + MESSAGES_BEFORE_SENTINEL + " data messages.");
            long start = System.currentTimeMillis();
            List<RmMessage> list = rm.fetch(RESPOND_SUBJECT, "rqst".getBytes());
            long elapsed = System.currentTimeMillis() - start;
            report(list);
            System.out.println("   Count: " + list.size() + ", Elapsed: " + elapsed + " ms");

            // Standard Sentinel can also work with RequestMany.iterate
            // Iterate gets an EOD here, even with a sentinel just because that's its pattern.
            System.out.println("\nIterate, Expect " + MESSAGES_BEFORE_SENTINEL + " data messages and EOD.");
            int ix = 0;
            start = System.currentTimeMillis();
            LinkedBlockingQueue<RmMessage> q = rm.iterate(RESPOND_SUBJECT, "rqst".getBytes());
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

    private static void report(List<RmMessage> list) {
        for (int x = 0; x < list.size(); x++) {
            RmMessage rmm = list.get(x);
            System.out.println("   "  + x + ". " + rmm);
        }
    }
}
