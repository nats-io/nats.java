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
import java.time.Duration;

public class MessageQueueBenchmark {
    public static void main(String args[]) throws InterruptedException {
        int msgCount = 10_000_000;
        NatsMessage[] msgs = new NatsMessage[msgCount];
        long start, end;

        System.out.printf("Running benchmarks with %s messages.\n", NumberFormat.getInstance().format(msgCount));
        for (int j = 0; j < msgCount; j++) {
            msgs[j] = new NatsMessage("a");
        }

        MessageQueue pushPop = new MessageQueue();
        start = System.nanoTime();
        for (int i = 0; i < msgCount; i++) {
            pushPop.push(msgs[i]);
        }
        for (int i = 0; i < msgCount; i++) {
            pushPop.popNow();
        }
        end = System.nanoTime();

        System.out.printf("\nTotal time to perform %s push/popnow operations was %s ms, %f ns/op\n",
                NumberFormat.getInstance().format(msgCount),
                NumberFormat.getInstance().format((end - start) / 1_000_000L),
                ((double) (end - start)) / ((double) (msgCount)));
        System.out.printf("\tor %s op/s\n",
                NumberFormat.getInstance().format(1_000_000_000L * ((double) (msgCount))/((double) (end - start))));

        MessageQueue accumulateQueue = new MessageQueue();
        start = System.nanoTime();
        for (int i = 0; i < msgCount; i++) {
            accumulateQueue.push(msgs[i]);
        }
        while(accumulateQueue.length() > 0) { // works for single thread, but not multi
            accumulateQueue.accumulate(10_000, 100, Duration.ofMillis(500));
        }
        end = System.nanoTime();

        System.out.printf("\nTotal time to perform %s push/accumulate operations was %s ms, %f ns/op\n",
                NumberFormat.getInstance().format(msgCount),
                NumberFormat.getInstance().format((end - start) / 1_000_000L),
                ((double) (end - start)) / ((double) (msgCount)));
            System.out.printf("\tor %s op/s\n",
                    NumberFormat.getInstance().format(1_000_000_000L * ((double) (msgCount))/((double) (end - start))));
        
        final MessageQueue pushPopThreadQueue = new MessageQueue();
        final Duration timeout = Duration.ofMillis(10);
        Thread pusher = new Thread(() -> {
            for (int i = 0; i < msgCount; i++) {
                pushPopThreadQueue.push(msgs[i]);
            }
        });

        Thread popper = new Thread(() -> {
            try {
                for (int i = 0; i < msgCount; i++) {
                    pushPopThreadQueue.pop(timeout);
                }
            } catch (Exception exp) {
                exp.printStackTrace();
            }
        });

        start = System.nanoTime();
        pusher.start();
        popper.start();
        pusher.join();
        popper.join();
        end = System.nanoTime();

        System.out.printf("\nTotal time to perform %s pushes in one thread and pop with timeout in another was %s ms, %f ns/op\n",
                NumberFormat.getInstance().format(msgCount),
                NumberFormat.getInstance().format((end - start) / 1_000_000L),
                ((double) (end - start)) / ((double) (msgCount)));
            System.out.printf("\tor %s op/s\n",
                    NumberFormat.getInstance().format(1_000_000_000L * ((double) (msgCount))/((double) (end - start))));
        


        final MessageQueue pushPopNowThreadQueue = new MessageQueue();
        pusher = new Thread(() -> {
            for (int i = 0; i < msgCount; i++) {
                pushPopNowThreadQueue.push(msgs[i]);
            }
        });

        popper = new Thread(() -> {
            try {
                for (int i = 0; i < msgCount; i++) {
                    pushPopNowThreadQueue.popNow();
                }
            } catch (Exception exp) {
                exp.printStackTrace();
            }
        });

        start = System.nanoTime();
        pusher.start();
        popper.start();
        pusher.join();
        popper.join();
        end = System.nanoTime();

        System.out.printf("\nTotal time to perform %s pushes in one thread and pop nows in another was %s ms, %f ns/op\n",
                NumberFormat.getInstance().format(msgCount),
                NumberFormat.getInstance().format((end - start) / 1_000_000L),
                ((double) (end - start)) / ((double) (msgCount)));
            System.out.printf("\tor %s op/s\n",
                    NumberFormat.getInstance().format(1_000_000_000L * ((double) (msgCount))/((double) (end - start))));
                    
        final MessageQueue pushAccumulateThreadQueue = new MessageQueue();
        pusher = new Thread(() -> {
            for (int i = 0; i < msgCount; i++) {
                pushAccumulateThreadQueue.push(msgs[i]);
            }
        });

        popper = new Thread(() -> {
            try {
                int remaining = msgCount;
                while (remaining > 0) {
                    NatsMessage cursor = pushAccumulateThreadQueue.accumulate(10_000, 100, Duration.ofMillis(500));
                    while (cursor != null) {
                        remaining--;
                        cursor = cursor.next;
                    }
                }
            } catch (Exception exp) {
                exp.printStackTrace();
            }
        });

        start = System.nanoTime();
        pusher.start();
        popper.start();
        pusher.join();
        popper.join();
        end = System.nanoTime();

        System.out.printf("\nTotal time to perform %s pushes in one thread and accumlates in another was %s ms, %f ns/op\n",
                NumberFormat.getInstance().format(msgCount),
                NumberFormat.getInstance().format((end - start) / 1_000_000L),
                ((double) (end - start)) / ((double) (msgCount)));
            System.out.printf("\tor %s op/s\n",
                    NumberFormat.getInstance().format(1_000_000_000L * ((double) (msgCount))/((double) (end - start))));
    }
}