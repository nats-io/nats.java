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
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;


public class PublishBenchmarkWithStats {
    public static void main(String args[]) throws InterruptedException {
        int threads = 1;
        int msgsPerThread = 5_000_000;
        int messageSize = 256;
        long totalMessages = threads * msgsPerThread;
        CountDownLatch latch = new CountDownLatch(threads);
        CompletableFuture<Boolean> starter = new CompletableFuture<>();

        System.out.println("###");
        System.out.printf("### Running publish benchmark with %s %s byte messages across %s threads.\n",
                                NumberFormat.getInstance().format(totalMessages),
                                NumberFormat.getInstance().format(messageSize),
                                NumberFormat.getInstance().format(threads));
        System.out.println("###");
        byte[] body = new byte[messageSize];

        for(int i=0; i<messageSize; i++) {
            body[i] = 1;
        }

        try {
            Options options = new Options.Builder().server(Options.DEFAULT_URL).turnOnAdvancedStats().build();
            Connection nc = Nats.connect(options);

            for (int k = 0;k<threads;k++) {
                Thread t = new Thread(() -> {
                    try {starter.get();}catch(Exception e){}
                    for(int i = 0; i < msgsPerThread; i++) {
                        nc.publish("bench", body);
                    }
                    try {nc.flush(Duration.ZERO);}catch(Exception e){}
                    latch.countDown();
                });
                t.start();
            }

            long start = System.nanoTime();
            starter.complete(Boolean.TRUE);
            latch.await();
            long end = System.nanoTime();

            nc.close();

            System.out.printf("### Total time to perform %s operations was %s ms, %f ns/op\n",
                NumberFormat.getInstance().format(totalMessages), 
                NumberFormat.getInstance().format((end-start)/1_000_000L),
                ((double)(end-start))/((double)(totalMessages)));
            System.out.printf("### This is equivalent to %s msg/sec.\n",
                NumberFormat.getInstance().format(1_000_000_000L * totalMessages/(end-start)));
            System.out.printf("### Each operation consists of a publish of a msg of size %s.\n",
                NumberFormat.getInstance().format(messageSize));
            System.out.printf("### %s thread(s) were used.\n",
                NumberFormat.getInstance().format(threads));

            System.out.println("###");
            System.out.println("### Overall Statistics");
            System.out.println();
            System.out.print(nc.getStatistics().toString());
        } catch (Exception ex) {
            System.out.println("Exception running benchmark.");
            ex.printStackTrace();
        }
    }
}