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

package io.nats.client.impl;

import java.text.NumberFormat;

import org.junit.Test;

// TODO(sasbury): Create an issue to update to JMH.
public class MessageQueueBenchmark {
    @Test
    public void pushPopBenchmark() throws InterruptedException {

        int loops = 1000;
        int msgPerLoop = 1_000_000;
        MessageQueue q = new MessageQueue();
        NatsMessage[] msgs = new NatsMessage[msgPerLoop];

        for (int j=0;j<msgPerLoop;j++) {
            msgs[j] = new NatsMessage("");
        }

        long start = System.nanoTime();
        for(int i = 0; i < loops; i++) {
            for (int j = 0; j< loops; j++) {
                q.push(msgs[j]);
            }
            for (int j = 0; j< loops; j++) {
                q.popNow();
            }
        }
        long end = System.nanoTime();

        System.out.printf("\n### Total time to perform %s operations was %s ms, %f ns/op\n",
            NumberFormat.getInstance().format(loops*msgPerLoop), 
            NumberFormat.getInstance().format( (end-start)/1_000_000L),
            ((double)(end-start))/((double)(loops*msgPerLoop)));
        System.out.printf("### Each operation consists of a push/pop pair in groups of %s.\n\n",
            NumberFormat.getInstance().format(msgPerLoop));

    }
}