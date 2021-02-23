// Copyright 2020 The NATS Authors
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

package io.nats.examples;

import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.impl.JetStreamApiException;
import io.nats.client.impl.NatsMessage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

abstract class NatsJsPullSubBase {

    public static void publish(JetStream js, String subject, String prefix, int count) throws IOException, JetStreamApiException {
        System.out.print("Publish ->");
        for (int x = 1; x <= count; x++) {
            String data = "#" + prefix + x;
            System.out.print(" " + data);
            Message msg = NatsMessage.builder()
                    .subject(subject)
                    .data(data.getBytes(StandardCharsets.US_ASCII))
                    .build();
            js.publish(msg);
        }
        System.out.println(" <-");
    }

    public static List<Message> readMessagesAck(JetStreamSubscription sub) throws InterruptedException {
        List<Message> messages = new ArrayList<>();
        Message msg = sub.nextMessage(Duration.ofSeconds(1));
        boolean first = true;
        while (msg != null) {
            if (first) {
                first = false;
                System.out.print("Read/Ack ->");
            }
            messages.add(msg);
            if (msg.isJetStream()) {
                msg.ack();
                System.out.print(" " + new String(msg.getData()));
            }
            else if (msg.isStatusMessage()){
                System.out.print(" !" + msg.getStatus().getCode() + "!");
            }
            else {
                System.out.print(" ?" + new String(msg.getData()) + "?");
            }
            msg = sub.nextMessage(Duration.ofSeconds(1));
        }
        if (first) {
            System.out.println("No messages available.");
        }
        else {
            System.out.println(" <- ");
        }
        return messages;
    }
}
