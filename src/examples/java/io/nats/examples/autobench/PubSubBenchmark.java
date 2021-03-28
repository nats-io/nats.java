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

package io.nats.examples.autobench;

import io.nats.client.*;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class PubSubBenchmark extends ThrottledBenchmark {

    public PubSubBenchmark(String name, long messageCount, long messageSize) {
        super(name, messageCount, messageSize);
    }

    void executeWithLimiter(Options connectOptions) throws InterruptedException {
        byte[] payload = createPayload();
        String subject = getSubject();

        final CompletableFuture<Void> go = new CompletableFuture<>();
        final CompletableFuture<Void> subReady = new CompletableFuture<>();
        final CompletableFuture<Void> pubReady = new CompletableFuture<>();
        final CompletableFuture<Void> subDone = new CompletableFuture<>();
        final CompletableFuture<Void> pubDone = new CompletableFuture<>();

        Thread subThread = new Thread(() -> {
            try {
                int count = 0;
                Connection subConnect = Nats.connect(connectOptions);

                if (subConnect.getStatus() != Connection.Status.CONNECTED) {
                    throw new Exception("Unable to connect");
                }
                try {
                    Subscription sub = subConnect.subscribe(subject);
                    defaultFlush(subConnect);
                    subReady.complete(null);
                    
                    while(count < this.getMessageCount()) {
                        Message msg = sub.nextMessage(Duration.ofSeconds(5));

                        if (msg != null){
                            count++;
                        } else {
                            throw new Exception("No messages within timeout.");
                        }
                    }

                    subDone.complete(null);
                } catch (Exception exp) {
                    this.setException(exp);
                } finally {
                    subConnect.close();
                }
            } catch (Exception ex) {
                subReady.cancel(true);
                this.setException(ex);
            } finally {
                subDone.complete(null);
            }
        }, "PubSub Test - Subscriber");
        subThread.start();

        Thread pubThread = new Thread(() -> {
            try {
                Connection pubConnect = Nats.connect(connectOptions);
                if (pubConnect.getStatus() != Connection.Status.CONNECTED) {
                    throw new Exception("Unable to connect");
                }
                try {
                    pubReady.complete(null);
                    go.get();
                    
                    for(int i = 0; i < this.getMessageCount(); i++) {
                        pubConnect.publish(subject, payload);
                        adjustAndSleep(pubConnect);
                    }
                    try {pubConnect.flush(Duration.ofSeconds(5));}catch(Exception e){}
                    
                    pubDone.complete(null);
                } finally {
                    pubConnect.close();
                }
            } catch (Exception ex) {
                pubReady.cancel(true);
                this.setException(ex);
                pubFailed();
            } finally {
                pubDone.complete(null);
            }
        }, "PubSub Test - Publisher");
        pubThread.start();

        getFutureSafely(subReady);
        getFutureSafely(pubReady);

        if (this.getException() != null) {
            go.complete(null); // just in case the other thread is waiting
            return;
        }
        
        startTiming();
        go.complete(null);
        getFutureSafely(pubDone);
        getFutureSafely(subDone);
        endTiming();

        pubThread.join();
        subThread.join();
    }
}