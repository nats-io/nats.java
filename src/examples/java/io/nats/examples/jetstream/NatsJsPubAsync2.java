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

package io.nats.examples.jetstream;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.NatsMessage;
import io.nats.examples.ExampleArgs;
import io.nats.examples.ExampleUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * This example will demonstrate JetStream async / future publishing where the pub ack handling is in a separate thread.
 */
public class NatsJsPubAsync2 {
    static final String usageString =
            "\nUsage: java -cp <classpath> NatsJsPubAsync2 [-s server] [-strm stream] [-sub subject] [-mcnt msgCount] [-m messageWords+] [-r headerKey:headerValue]*"
                    + "\n\nDefault Values:"
                    + "\n   [-strm] example-stream"
                    + "\n   [-sub]  example-subject"
                    + "\n   [-mcnt] 10"
                    + "\n   [-m] hello"
                    + "\n\nRun Notes:"
                    + "\n   - msg_count < 1 is the same as 1"
                    + "\n   - headers are optional"
                    + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
                    + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
                    + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
                    + "\nUse the URL in the -s server parameter for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder("Publish Async 2", args, usageString)
                .defaultStream("example-stream")
                .defaultSubject("example-subject")
                .defaultMessage("hello")
                .defaultMsgCount(10)
                .build();

        String hdrNote = exArgs.hasHeaders() ? ", with " + exArgs.headers.size() + " header(s)" : "";
        System.out.printf("\nPublishing to %s%s. Server is %s\n\n", exArgs.subject, hdrNote, exArgs.server);

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server))) {

            // Create a JetStream context.  This hangs off the original connection
            // allowing us to produce data to streams and consume data from
            // JetStream consumers.
            JetStream js = nc.jetStream();

            // See the NatsJsManageStreams example or the NatsJsUtils for examples on how to create the stream
            NatsJsUtils.createStreamOrUpdateSubjects(nc, exArgs.stream, exArgs.subject);

            class Helper {
                Message msg;
                CompletableFuture<PublishAck> future;
            }

            int stop = exArgs.msgCount < 2 ? 2 : exArgs.msgCount + 1;
            CountDownLatch ackLatch = new CountDownLatch(stop - 1);
            LinkedBlockingQueue<Helper> queue = new LinkedBlockingQueue<>();
            LinkedBlockingQueue<Helper> redo = new LinkedBlockingQueue<>();

            new Thread(() -> {
                while (ackLatch.getCount() > 0) {
                    Helper h;
                    try {
                        // get an item from the queue
                        h = queue.take();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    try {
                        // if the future is done
                        if (h.future.isDone()) {
                            PublishAck pa = h.future.get();
                            // if it gets here the ack is fine
                            System.out.println("Pub ack received " + pa);
                            ackLatch.countDown();
                        }
                        else {
                            // not done yet, put it back in the queue
                            // don't count it b/c we are not done with it.
                            queue.add(h);
                        }
                    } catch (InterruptedException e) {
                        redo.add(h);
                        ackLatch.countDown();
                        // do something
                    } catch (ExecutionException e) {
                        // do something, the ack probably failed.
                        // may need to republish, the helper has the message
                        redo.add(h);
                        ackLatch.countDown();
                    }
                }
            }).start();

            new Thread(() -> {
                for (int x = 1; x < stop; x++) {
                    // make unique message data if you want more than 1 message
                    String data = exArgs.msgCount < 2 ? exArgs.message : exArgs.message + "-" + x;

                    // create a typical NATS message
                    Message msg = NatsMessage.builder()
                        .subject(exArgs.subject)
                        .headers(exArgs.headers)
                        .data(data, StandardCharsets.UTF_8)
                        .build();
                    System.out.printf("Publishing message %s on subject %s.\n", data, exArgs.subject);

                    // Publish a message, put it in a helper along with the future
                    Helper h = new Helper();
                    h.msg = msg;
                    h.future = js.publishAsync(msg);
                    queue.add(h);
                }

                // if all the acks haven't finished, we should check for redo
                while (ackLatch.getCount() > 0) {
                    try {
                        Helper h = redo.poll(10, TimeUnit.MILLISECONDS);
                        if (h != null) {
                            System.out.printf("RE publishing message %s.\n", new String(h.msg.getData()));
                            h.future = js.publishAsync(h.msg);
                            queue.add(h);
                        }
                    } catch (InterruptedException e) {
                        // ignore or do something
                    }
                }
            }).start();

            ackLatch.await();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
