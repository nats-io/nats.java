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

package io.nats.client.impl;

import io.nats.client.*;
import io.nats.client.PullSubscribeOptions.AckMode;
import io.nats.client.PullSubscribeOptions.ExpireMode;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class JetStreamPullBehavior extends JetStreamTestBase {

    static final int SIZE = 4;
    static final int[] PUBs = new int[]{4, 8, 6, 2, 0};

    static final boolean[] BOOLs = new boolean[]{true, false};

    static FileOutputStream csv;
    static Connection nc;
    static JetStreamManagement jsm;
    static JetStream js;

    public static void main(String[] args) throws Exception {
        try (FileOutputStream _csv = new FileOutputStream("C:\\nats\\pull-behavior.csv")) {
            csv = _csv;
            runGroup("Plain + Next", AckMode.NEXT, null, sub -> sub.pull(SIZE));
            runGroup("Plain + Ack", AckMode.ACK, null, sub -> sub.pull(SIZE));
            runGroup("No Wait + Next", AckMode.NEXT, null, sub -> sub.pullNoWait(SIZE));
            runGroup("No Wait + Ack", AckMode.ACK, null, sub -> sub.pullNoWait(SIZE));
            runGroup("Expire + Next + Advance", AckMode.NEXT, ExpireMode.ADVANCE, sub -> sub.pullExpiresIn(SIZE, Duration.ofSeconds(2)));
            runGroup("Expire + Next + Advance + Sleep", AckMode.NEXT, ExpireMode.ADVANCE, sub -> sub.pullExpiresIn(SIZE, Duration.ofSeconds(2)), 2000);
            runGroup("Expire + Next + Leave", AckMode.NEXT, ExpireMode.LEAVE, sub -> sub.pullExpiresIn(SIZE, Duration.ofSeconds(2)));
            runGroup("Expire + Next + Leave + Sleep", AckMode.NEXT, ExpireMode.LEAVE, sub -> sub.pullExpiresIn(SIZE, Duration.ofSeconds(2)), 2000);
            runGroup("Expire + Ack", AckMode.ACK, null, sub -> sub.pullExpiresIn(SIZE, Duration.ofSeconds(2)));
            runGroup("Expire + Ack + Sleep", AckMode.ACK, null, sub -> sub.pullExpiresIn(SIZE, Duration.ofSeconds(2)), 2000);
            runGroup("Expire + Past + Next + Advance", AckMode.NEXT, ExpireMode.ADVANCE, sub -> sub.pullExpiresIn(SIZE, Duration.ofSeconds(2)));
            runGroup("Expire + Past + Next + Leave", AckMode.NEXT, ExpireMode.LEAVE, sub -> sub.pullExpiresIn(SIZE, Duration.ofSeconds(2)));
            runGroup("Expire + Past + Ack", AckMode.ACK, null, sub -> sub.pullExpiresIn(SIZE, Duration.ofSeconds(2)));
        }
    }

    private static void runGroup(String label, AckMode ackMode, ExpireMode expireMode, Pull pull) throws Exception {
        runGroup(label, ackMode, expireMode, pull, 0);
    }

    private static void runGroup(String label, AckMode ackMode, ExpireMode expireMode, Pull pull, long sleep) throws Exception {
        runInJsServer(_nc -> {
            nc = _nc;
            jsm = _nc.jetStreamManagement();
            js = _nc.jetStream();
            write(label + HEADER_SLEEP);
            boolean first = true;
            for (int pub : PUBs) {
                int[] REPUBs = pub == 2 ? new int[]{4, 6} : new int[]{4};
                for (int repub : REPUBs) {
                    for (boolean repull : BOOLs) {
                        runOne(first, ackMode, expireMode, pub, repub, repull, pull, sleep);
                        first = false;
                    }
                }
            }
            write(null);
        });
    }

    static final String HEADER_SLEEP = ",Ack,Expire,Pub,RePull,Msgs 1,Msgs 2";

    public static void runOne(boolean first, AckMode ackMode, ExpireMode expireMode, int pub, int repub, boolean repull, Pull pull, long sleep) throws Exception {
        long unique = System.currentTimeMillis();
        String stream = STREAM + unique;
        String subject = SUBJECT + unique;
        String durable = DURABLE + unique;
        createMemoryStream(jsm, stream, subject);
        PullSubscribeOptions options = PullSubscribeOptions.builder()
                .durable(durable).ackMode(ackMode).expireMode(expireMode).build();
        JetStreamSubscription sub = js.subscribe(subject, options);
        nc.flush(Duration.ofSeconds(10));

        StringBuilder sb = new StringBuilder();
        if (first) {
            append(sb, ackMode);
            append(sb, expireMode);
        }
        else {
            sb.append(",,");
        }
        append(sb, "" + pub + "," + repub);
        append(sb, repull);

        System.out.println(sb.toString());
        if (pub > 0) {
            pub(js, subject, "A", pub);
        }
        pull.pull(sub);
        sleep(sleep);

        List<Message> messages = readMessagesAck(sub);
        String ms = received(messages);
        System.out.println("1. " + ms);
        append(sb, ms);

        pub(js, subject, "B", repub);

        if (repull) {
            pull.pull(sub);
        }
        sleep(sleep);

        messages = readMessagesAck(sub);
        ms = received(messages);
        System.out.println("2. " + ms);
        append(sb, ms);

        write(sb.toString());
    }

    private static void append(StringBuilder sb, boolean b) {
        sb.append(b ? ",Yes" : ",No");
    }

    private static void append(StringBuilder sb, long l) {
        sb.append(",").append(l);
    }

    private static void append(StringBuilder sb, String s) {
        if (s.contains(" ") || s.contains(",")) {
            sb.append(",\"").append(s).append('"');
        }
        else {
            sb.append(",").append(s);
        }
    }

    private static void append(StringBuilder sb, Enum<?> e) {
        if (e == null) {
            sb.append(",n/a");
        }
        else {
            append(sb, e.toString());
        }
    }

    private static String received(List<Message> messages) {
        if (messages.size() == 0) {
            return "No Msgs";
        }

        MessageMetaData meta = null;
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Message m : messages) {
            if (first) {
                first = false;
            }
            else {
                sb.append(" ");
            }
            if (m.isStatusMessage()) {
                sb.append("!").append(m.getStatus().getCode());
            }
            else if (m.isJetStream()){
                meta = m.metaData();
                sb.append(new String(m.getData()));
            }
            else {
                sb.append("?");
            }
        }
        return sb.toString();
    }

    public static void pub(JetStream js, String subject, String id, int count) throws IOException, JetStreamApiException {
        for (int x = 0; x < count; x++) {
            String data = id + (x+1);
            js.publish(NatsMessage.builder().subject(subject).data(data.getBytes()).build());
        }
    }

    public static List<Message> readMessagesAck(JetStreamSubscription sub) throws InterruptedException {
        List<Message> messages = new ArrayList<>();
        Message msg = sub.nextMessage(Duration.ofSeconds(1));
        while (msg != null) {
            messages.add(msg);
            if (msg.isJetStream()) {
                msg.ack();
            }
            msg = sub.nextMessage(Duration.ofSeconds(1));
        }
        return messages;
    }

    private static void write(String s) throws Exception {
        if (s != null) {
            csv.write(s.getBytes(StandardCharsets.US_ASCII));
        }
        csv.write("\n".getBytes(StandardCharsets.US_ASCII));
    }

    public interface Pull {
        void pull(JetStreamSubscription sub) throws Exception;
    }
}
