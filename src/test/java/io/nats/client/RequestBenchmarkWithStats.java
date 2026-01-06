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

package io.nats.client;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


public class RequestBenchmarkWithStats {
    public static void main(String args[]) throws InterruptedException {
        int threads = 1;
        int msgsPerThread = 5_000_000;
        int messageSize = 256;
        long totalMessages = threads * msgsPerThread;
        CountDownLatch latch = new CountDownLatch(threads);
        CompletableFuture<Boolean> starter = new CompletableFuture<>();
        AtomicLong msgsHandled = new AtomicLong();
        AtomicLong futureExceptions = new AtomicLong();

        System.out.println("###");
        System.out.printf("### Running request benchmark with %s %s byte messages across %s threads.\n",
                                NumberFormat.getInstance().format(totalMessages),
                                NumberFormat.getInstance().format(messageSize),
                                NumberFormat.getInstance().format(threads));
        System.out.println("###");

        byte[] body = new byte[messageSize];
        for (int i = 0; i < messageSize; i++) {
            body[i] = 1;
        }

        try {
            Options o = new Options.Builder().
                        server(Options.DEFAULT_URL).
                        turnOnAdvancedStats().
                        build();

            Connection handlerC = Nats.connect(o);
            Dispatcher d = handlerC.createDispatcher((msg) -> {
                try {
                    handlerC.publish(msg.getReplyTo(), msg.getData());
                    msgsHandled.incrementAndGet();
                } catch (Exception exp) {
                    exp.printStackTrace();
                }
            });
            d.subscribe("req");

            Connection nc = Nats.connect(o);

            for (int k = 0; k < threads; k++) {
                Thread t = new Thread(() -> {

                    try {
                        starter.get();
                    } catch (Exception e) {
                    }

                    ArrayList<Future<Message>> msgs = new ArrayList<>();
                    for (int i = 0; i < msgsPerThread; i++) {
                        Future<Message> msg = nc.request("req", body);
                        msgs.add(msg);

                        if (i!=0 && i%1_000==0) {
                            for (Future<Message> m : msgs) {
                                try {
                                    m.get(100, TimeUnit.MILLISECONDS);
                                } catch (Exception e) {
                                    futureExceptions.incrementAndGet();
                                }
                            }

                            msgs.clear();
                        }
                    }

                    try {
                        nc.flush(null);
                        handlerC.flush(null);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    for (Future<Message> m : msgs) {
                        try {
                            m.get(100, TimeUnit.MILLISECONDS);
                        } catch (Exception e) {
                            futureExceptions.incrementAndGet();
                        }
                    }
                    msgs.clear();

                    latch.countDown();
                });
                t.start();
            }

            long start = System.nanoTime();
            starter.complete(Boolean.TRUE);
            latch.await();
            long end = System.nanoTime();

            nc.close();
            handlerC.close();

            System.out.printf("### Total time to perform %s request-replies was %s ms, %f ns/op\n",
                    NumberFormat.getInstance().format(totalMessages),
                    NumberFormat.getInstance().format((end - start) / 1_000_000L),
                    ((double) (end - start)) / ((double) (totalMessages)));
            System.out.printf("### This is equivalent to %s request-replies/sec.\n",
                    NumberFormat.getInstance().format(1_000_000_000L * totalMessages / (end - start)));
            System.out.printf("### Messages were of size %s.\n",
                    NumberFormat.getInstance().format(messageSize));
            System.out.printf("### %s thread(s) were used. A single dispatcher handles all messages.\n",
                    NumberFormat.getInstance().format(threads));
            System.out.printf("### Dispatcher handled %s messages.\n",
                    NumberFormat.getInstance().format(msgsHandled.get()));
            System.out.printf("### %s exceptions waiting on futures.\n",
                    NumberFormat.getInstance().format(futureExceptions.get()));

            System.out.println("#################################");
            System.out.println("### Request connection stats ####");
            System.out.println("#################################");
            System.out.println();
            System.out.print(nc.getStatistics().toString());
            System.out.println();
            System.out.println("####################################");
            System.out.println("### Dispatcher connection stats ####");
            System.out.println("####################################");
            System.out.println("");
            System.out.print(handlerC.getStatistics().toString());
        } catch (Exception ex) {
            System.out.println("Exception running benchmark.");
            ex.printStackTrace();
        }
    }
}