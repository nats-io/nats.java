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

package io.nats.client.other;

/*
import io.nats.client.impl.MessageQueue;
import io.nats.client.impl.NatsMessage;
import io.nats.client.impl.ProtocolMessage;

import java.text.NumberFormat;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
*/

// This class is kept only for history. It can only run if it is moved to the
// io.nats.client.impl package since MessageQueue is package scoped
public class MessageQueueBenchmark {
/*
    static final Duration REQUEST_CLEANUP_INTERVAL = Duration.ofSeconds(5);
    
    public static void main(String args[]) throws InterruptedException {
        int msgCount = 10_000_000;
        NatsMessage[] msgs = new NatsMessage[msgCount];
        long start, end;

        System.out.printf("Running benchmarks with %s messages.\n", NumberFormat.getInstance().format(msgCount));
        System.out.println("Warmed up ...");
        byte[] warmBytes = "a".getBytes();

        MessageQueue warm = new MessageQueue(false, REQUEST_CLEANUP_INTERVAL);
        for (int j = 0; j < msgCount; j++) {
            msgs[j] = new ProtocolMessage(warmBytes);
            warm.push(msgs[j]);
        }

        System.out.println("Starting tests ...");
        MessageQueue push = new MessageQueue(false, REQUEST_CLEANUP_INTERVAL);
        start = System.nanoTime();
        for (int i = 0; i < msgCount; i++) {
            push.push(msgs[i]);
        }
        end = System.nanoTime();

        System.out.printf("\nTotal time to perform %s push operations was %s ms, %s ns/op\n",
                NumberFormat.getInstance().format(msgCount),
                NumberFormat.getInstance().format((end - start) / 1_000_000L),
                NumberFormat.getInstance().format(((double) (end - start)) / ((double) (msgCount))));
        System.out.printf("\tor %s op/s\n",
                NumberFormat.getInstance().format(1_000_000_000L * ((double) (msgCount))/((double) (end - start))));

        start = System.nanoTime();
        for (int i = 0; i < msgCount; i++) {
            push.popNow();
        }
        end = System.nanoTime();

        System.out.printf("\nTotal time to perform %s popnow operations was %s ms, %s ns/op\n",
                NumberFormat.getInstance().format(msgCount),
                NumberFormat.getInstance().format((end - start) / 1_000_000L),
                NumberFormat.getInstance().format(((double) (end - start)) / ((double) (msgCount))));
        System.out.printf("\tor %s op/s\n",
                NumberFormat.getInstance().format(1_000_000_000L * ((double) (msgCount))/((double) (end - start))));

        MessageQueue accumulateQueue = new MessageQueue(true, REQUEST_CLEANUP_INTERVAL);
        for (int j = 0; j < msgCount; j++) {
            msgs[j].next = null;
        }
        for (int i = 0; i < msgCount; i++) {
            accumulateQueue.push(msgs[i]);
        }
        start = System.nanoTime();
        while(accumulateQueue.length() > 0) { // works for single thread, but not multi
            accumulateQueue.accumulate(10_000, 100, Duration.ofMillis(500));
        }
        end = System.nanoTime();

        System.out.printf("\nTotal time to perform accumulate %s messages was %s ms, %s ns/op\n",
                NumberFormat.getInstance().format(msgCount),
                NumberFormat.getInstance().format((end - start) / 1_000_000L),
                NumberFormat.getInstance().format(((double) (end - start)) / ((double) (msgCount))));
            System.out.printf("\tor %s op/s\n",
                    NumberFormat.getInstance().format(1_000_000_000L * ((double) (msgCount))/((double) (end - start))));
        
        for (int j = 0; j < msgCount; j++) {
            msgs[j].next = null;
        }
        final MessageQueue pushPopThreadQueue = new MessageQueue(false, REQUEST_CLEANUP_INTERVAL);
        final Duration timeout = Duration.ofMillis(10);
        final CompletableFuture<Void> go = new CompletableFuture<>();
        Thread pusher = new Thread(() -> {
            try {
                go.get();
                for (int i = 0; i < msgCount; i++) {
                    pushPopThreadQueue.push(msgs[i]);
                }
            } catch (Exception exp) {
                exp.printStackTrace();
            }
        });
        pusher.start();

        Thread popper = new Thread(() -> {
            try {
                go.get();
                for (int i = 0; i < msgCount; i++) {
                    pushPopThreadQueue.pop(timeout);
                }
            } catch (Exception exp) {
                exp.printStackTrace();
            }
        });
        popper.start();

        start = System.nanoTime();
        go.complete(null);
        pusher.join();
        popper.join();
        end = System.nanoTime();

        System.out.printf("\nTotal time to perform %s pushes in one thread and pop with timeout in another was %s ms, %s ns/op\n",
                NumberFormat.getInstance().format(msgCount),
                NumberFormat.getInstance().format((end - start) / 1_000_000L),
                NumberFormat.getInstance().format(((double) (end - start)) / ((double) (msgCount))));
            System.out.printf("\tor %s op/s\n",
                    NumberFormat.getInstance().format(1_000_000_000L * ((double) (msgCount))/((double) (end - start))));
        
        final CompletableFuture<Void> go2 = new CompletableFuture<>();
        for (int j = 0; j < msgCount; j++) {
            msgs[j].next = null;
        }
        final MessageQueue pushPopNowThreadQueue = new MessageQueue(false, REQUEST_CLEANUP_INTERVAL);
        pusher = new Thread(() -> {
            try {
                go2.get();
                for (int i = 0; i < msgCount; i++) {
                    pushPopNowThreadQueue.push(msgs[i]);
                }
            } catch (Exception exp) {
                exp.printStackTrace();
            }
        });
        pusher.start();

        popper = new Thread(() -> {
            try {
                go2.get();
                for (int i = 0; i < msgCount; i++) {
                    pushPopNowThreadQueue.popNow();
                }
            } catch (Exception exp) {
                exp.printStackTrace();
            }
        });
        popper.start();

        start = System.nanoTime();
        go2.complete(null);
        pusher.join();
        popper.join();
        end = System.nanoTime();

        System.out.printf("\nTotal time to perform %s pushes in one thread and pop nows in another was %s ms, %s ns/op\n",
                NumberFormat.getInstance().format(msgCount),
                NumberFormat.getInstance().format((end - start) / 1_000_000L),
                NumberFormat.getInstance().format(((double) (end - start)) / ((double) (msgCount))));
            System.out.printf("\tor %s op/s\n",
                    NumberFormat.getInstance().format(1_000_000_000L * ((double) (msgCount))/((double) (end - start))));
            
        final CompletableFuture<Void> go3 = new CompletableFuture<>();
        for (int j = 0; j < msgCount; j++) {
            msgs[j].next = null;
        }

        final MessageQueue pushAccumulateThreadQueue = new MessageQueue(true, REQUEST_CLEANUP_INTERVAL);
        pusher = new Thread(() -> {
            try {
                go3.get();
                for (int i = 0; i < msgCount; i++) {
                    pushAccumulateThreadQueue.push(msgs[i]);
                }
            } catch (Exception exp) {
                exp.printStackTrace();
            }
        });
        pusher.start();

        popper = new Thread(() -> {
            try {
                go3.get();
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
        popper.start();

        start = System.nanoTime();
        go3.complete(null);
        pusher.join();
        popper.join();
        end = System.nanoTime();

        System.out.printf("\nTotal time to perform %s pushes in one thread and accumlates in another was %s ms, %s ns/op\n",
                NumberFormat.getInstance().format(msgCount),
                NumberFormat.getInstance().format((end - start) / 1_000_000L),
                NumberFormat.getInstance().format(((double) (end - start)) / ((double) (msgCount))));
            System.out.printf("\tor %s op/s\n",
                    NumberFormat.getInstance().format(1_000_000_000L * ((double) (msgCount))/((double) (end - start))));
    }
*/
}