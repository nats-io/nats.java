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

import java.util.ArrayList;
import java.util.List;

/**
 * Gather sends all messages to a handler.
 */
public class RequestManyGather {
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

            // On a gather...
            // If there was an exceptional reason for the completion,
            // an RmMessage with a status message or exception
            // and will be the last message added to the queue.
            // If there was no exception reason, a RmMessage.NORMAL_EOD will be added to the queue

            // We haven't started any responders yet so this will return with a Status 503
            // The good news is that a 503 (or any status or exception)
            // will short circuit and return quickly.
            // We should expect exactly 1 message since we know it's no responders.
            RequestMany rm = RequestMany.builder(nc).build();
            System.out.println("A. Expect 1 EOD Status message. ");
            System.out.println("   " + rm);

            List<RmMessage> list = new ArrayList<>();
            long start = System.currentTimeMillis();
            rm.gather(RESPOND_SUBJECT, null, list::add);
            long elapsed = System.currentTimeMillis() - start;
            report(list);
            System.out.println("   Count: " + list.size() + ", Elapsed: " + elapsed + " ms");

            // start a responder simulator. Each message it gets it will respond n times
            Dispatcher dispatcher = nc.createDispatcher(m -> {
                for (int x = 0; x < RESPONDERS; x++) {
                    nc.publish(m.getReplyTo(), ("R" + x).getBytes());
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

            list.clear();
            start = System.currentTimeMillis();
            rm.gather(RESPOND_SUBJECT, null, list::add);
            elapsed = System.currentTimeMillis() - start;
            report(list);
            System.out.println("   Count: " + list.size() + ", Elapsed: " + elapsed + " ms");

            // Maybe you always want to wait the full 1 second.
            // Maybe there could be slow (busy?) responders but you still want to hear from them.
            rm = RequestMany.builder(nc).totalWaitTime(1000).build();
            System.out.println("\nC. Expect " + RESPONDERS + " data messages and 1 EOD in slightly more than 1000 ms.");
            System.out.println("   " + rm);

            list.clear();
            start = System.currentTimeMillis();
            rm.gather(RESPOND_SUBJECT, null, list::add);
            elapsed = System.currentTimeMillis() - start;
            report(list);
            System.out.println("   Count: " + list.size() + ", Elapsed: " + elapsed + " ms");

            // Maybe you just want the first 2. Also limit the time. No slowpoke responders allowed!
            rm = RequestMany.builder(nc).totalWaitTime(1000).maxResponses(2).build();
            System.out.println("\nD. Expect 2 data messages and 1 EOD very quickly.");
            System.out.println("   " + rm);

            list.clear();
            start = System.currentTimeMillis();
            rm.gather(RESPOND_SUBJECT, null, list::add);
            elapsed = System.currentTimeMillis() - start;

            report(list);
            System.out.println("   Count: " + list.size() + ", Elapsed: " + elapsed + " ms");
        }
    }

    private static void report(List<RmMessage> list) {
        for (int x = 0; x < list.size(); x++) {
            RmMessage rmm = list.get(x);
            System.out.println("   "  + x + ". " + rmm);
        }
    }
}
