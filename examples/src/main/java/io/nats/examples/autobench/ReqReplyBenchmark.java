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
import java.util.concurrent.Future;

public class ReqReplyBenchmark extends AutoBenchmark {

    public ReqReplyBenchmark(String name, long messageCount, long messageSize) {
        super(name, messageCount, messageSize);
    }

    public void execute(Options connectOptions) throws InterruptedException {
        byte[] payload = createPayload();
        String subject = getSubject();

        final CompletableFuture<Void> go = new CompletableFuture<>();
        final CompletableFuture<Void> replyReady = new CompletableFuture<>();
        final CompletableFuture<Void> requestReady = new CompletableFuture<>();
        final CompletableFuture<Void> requestDone = new CompletableFuture<>();
        final CompletableFuture<Void> replyDone = new CompletableFuture<>();

        Thread replyThread = new Thread(() -> {
            try {
                Connection replyConnect = Nats.connect(connectOptions);
                if (replyConnect.getStatus() != Connection.Status.CONNECTED) {
                    throw new Exception("Unable to connect");
                }
                try {
                    Subscription sub = replyConnect.subscribe(subject);
                    defaultFlush(replyConnect);
                    replyReady.complete(null);
                    
                    int count = 0;
                    while(count < this.getMessageCount()) {
                        Message msg = sub.nextMessage(Duration.ofSeconds(5));

                        if (msg != null){
                            replyConnect.publish(msg.getReplyTo(), payload);
                            count++;
                        }
                    }
                    replyDone.complete(null);
                    replyConnect.flush(Duration.ofSeconds(5));
                    
                } catch (Exception exp) {
                    this.setException(exp);
                } finally {
                    replyConnect.close();
                }
            } catch (Exception ex) {
                replyReady.cancel(true);
                this.setException(ex);
            } finally {
                replyDone.complete(null);
            }
        }, "ReqReply Test - Reply");
        replyThread.start();

        Thread requestThread = new Thread(() -> {
            try {
                Connection requestConnect = Nats.connect(connectOptions);
                if (requestConnect.getStatus() != Connection.Status.CONNECTED) {
                    throw new Exception("Unable to connect");
                }
                try {
                    requestReady.complete(null);
                    go.get();
                    
                    for(int i = 0; i < this.getMessageCount(); i++) {
                        Future<Message> incoming = requestConnect.request(subject, payload);
                        incoming.get();
                    }
                    
                    requestDone.complete(null);
                } finally {
                    requestConnect.close();
                }
            } catch (Exception ex) {
                requestReady.cancel(true);
                this.setException(ex);
            } finally {
                requestDone.complete(null);
            }
        }, "ReqReply Test - Request");
        requestThread.start();

        getFutureSafely(replyReady);
        getFutureSafely(requestReady);

        if (this.getException() != null) {
            go.complete(null); // just in case the other thread is waiting
            return;
        }
        
        this.startTiming();
        go.complete(null);
        getFutureSafely(requestDone);
        getFutureSafely(replyDone);
        this.endTiming();
    }
}