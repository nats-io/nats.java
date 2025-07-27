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

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import io.nats.client.Options;

import java.text.NumberFormat;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class PubDispatchBenchmark extends ThrottledBenchmark {

    public PubDispatchBenchmark(String name, long messageCount, long messageSize) {
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
                Connection subConnect = Nats.connect(connectOptions);
                if (subConnect.getStatus() != Connection.Status.CONNECTED) {
                    throw new Exception("Unable to connect");
                }
                try {
                    AtomicInteger count = new AtomicInteger(0);
                    Dispatcher d = subConnect.createDispatcher((msg) -> {
                        if (count.incrementAndGet() >= this.getMessageCount()) {
                            subDone.complete(null);
                        }
                    });
                    d.subscribe(subject);
                    subConnect.flush(Duration.ofSeconds(5));
                    subReady.complete(null);
                    
                    // For simplicity the test doesn't have a connection listener so just loop
                    // we are async so otherwise we can't know if the connection closed under us
                    while (subConnect.getStatus() == Connection.Status.CONNECTED
                                && !subDone.isDone()) {
                        try {
                            subDone.get(100, TimeUnit.MILLISECONDS);
                        } catch (TimeoutException|CancellationException e){
                            //ignore these
                        }
                    }
                    
                    if (count.get() < this.getMessageCount()) {
                        throw new Exception("Dispatcher missed " + 
                                            NumberFormat.getIntegerInstance().format(this.getMessageCount() - count.get()) +
                                            " messages.");
                    }
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
        }, "PubDispatch Test - Subscriber");
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
                        this.adjustAndSleep(pubConnect);
                    }
                    defaultFlush(pubConnect);
                    pubDone.complete(null);
                } finally {
                    pubConnect.close();
                }
            } catch (Exception ex) {
                pubReady.cancel(true);
                this.setException(ex);
                this.pubFailed();
            } finally {
                pubDone.complete(null);
            }
        }, "PubDispatch Test - Publisher");
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
    }
}