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

package io.nats.examples.natsbyexample;

import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.impl.Headers;
import io.nats.examples.ExampleUtils;

import java.util.List;

/**
 * Key-Value Intro
 * The key-value (KV) capability in NATS is an abstraction over a stream which models message subjects as keys. It uses a standard set of stream configuration to be optimized for KV workloads.
 */
public class KeyValueIntro {

    public static void main(String[] args) {
        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions("nats://localhost:4222"))) {
            KeyValueManagement kvm = nc.keyValueManagement();

            // create the bucket
            KeyValueConfiguration kvc = KeyValueConfiguration.builder()
                .name("profiles")
                .storageType(StorageType.Memory)
                .build();

            KeyValueStatus keyValueStatus = kvm.create(kvc);

            KeyValue kv = nc.keyValue("profiles");

            kv.put("sue.color", "blue".getBytes());
            KeyValueEntry entry = kv.get("sue.color");
            System.out.printf("%s %d -> %s\n", entry.getKey(), entry.getRevision(), entry.getValueAsString());

            kv.put("sue.color", "green");
            entry = kv.get("sue.color");
            System.out.printf("%s %d -> %s\n", entry.getKey(), entry.getRevision(), entry.getValueAsString());

            try {
                kv.update("sue.color", "red".getBytes(), 1);
            }
            catch (JetStreamApiException e) {
                System.out.println(e);
            }

            long lastRevision = entry.getRevision();
            kv.update("sue.color", "red".getBytes(), lastRevision);
            entry = kv.get("sue.color");
            System.out.printf("%s %d -> %s\n", entry.getKey(), entry.getRevision(), entry.getValueAsString());

            JetStreamManagement jsm = nc.jetStreamManagement();

            List<String> streamNames = jsm.getStreamNames();
            System.out.println(streamNames);

            JetStream js = nc.jetStream();

            PushSubscribeOptions pso = PushSubscribeOptions.builder()
                .stream("KV_profiles").build();
            JetStreamSubscription sub = js.subscribe(">", pso);

            Message m = sub.nextMessage(100);
            System.out.printf("%s %d -> %s\n", m.getSubject(), m.metaData().streamSequence(), new String(m.getData()));

            kv.put("sue.color", "yellow".getBytes());
            m = sub.nextMessage(100);
            System.out.printf("%s %d -> %s\n", m.getSubject(), m.metaData().streamSequence(), new String(m.getData()));

            kv.delete("sue.color");
            m = sub.nextMessage(100);
            System.out.printf("%s %d -> %s\n", m.getSubject(), m.metaData().streamSequence(), new String(m.getData()));
            System.out.println("Headers:");
            Headers headers = m.getHeaders();
            for (String key : headers.keySet()) {
                System.out.printf("  %s:%s\n", key, headers.getFirst(key));
            }

            KeyValueWatcher watcher = new KeyValueWatcher() {
                @Override
                public void watch(KeyValueEntry entry) {
                    System.out.printf("Watcher: %s %d -> %s\n", entry.getKey(), entry.getRevision(), entry.getValueAsString());
                }

                @Override
                public void endOfData() {
                    System.out.println("Watcher: Received End Of Data Signal");
                }
            };

            kv.watch("sue.*", watcher, KeyValueWatchOption.UPDATES_ONLY);

            kv.put("sue.color", "purple");
        }
        catch (Exception exp) {
            exp.printStackTrace();
        }
    }
}
