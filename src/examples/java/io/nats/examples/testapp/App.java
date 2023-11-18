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

package io.nats.examples.testapp;

import io.nats.client.Connection;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;

import java.util.concurrent.CountDownLatch;

public class App {

    public static final String STREAM = "app-stream";
    public static final String SUBJECT = "app-subject";
    public static final String BOOTSTRAP = "nats://192.168.50.99:4222";
//    public static final String BOOTSTRAP = "nats://localhost:4222";

    public static void main(String[] args) throws Exception {
        try {
            Ui.start(Ui.Monitor.Left, false, false);
//            Debug.DEBUG_PRINTER = s -> {
//                System.out.println(s);
//                Ui.debugMessage(s);
//            };

            try (Connection nc = Nats.connect(BOOTSTRAP)) {
                JetStreamManagement jsm = nc.jetStreamManagement();
                createOrReplaceStream(jsm);
            }
            catch (Exception e) {
                Ui.controlMessage("APP", e.getMessage());
            }

            Publisher publisher = new Publisher(50);
            Thread pubThread = new Thread(publisher);
            pubThread.start();

            CountDownLatch waitForever = new CountDownLatch(1);

            SimpleConsumer simpleConsumer = new SimpleConsumer(true, 100, 5000);
            PushConsumer pushConsumer = new PushConsumer(true);

            Monitor monitor = new Monitor(publisher, simpleConsumer, pushConsumer);
            Thread monThread = new Thread(monitor);
            monThread.start();

            waitForever.await();
        }
        catch (Exception e) {
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
        }
    }

    public static void createOrReplaceStream(JetStreamManagement jsm) {
        try {
            jsm.deleteStream(STREAM);
        }
        catch (Exception ignore) {}
        try {
            StreamConfiguration sc = StreamConfiguration.builder()
                .name(STREAM)
                .storageType(StorageType.File)
                .subjects(SUBJECT)
                .build();
            StreamInfo si = jsm.addStream(sc);
            Ui.controlMessage("APP", si.getConfiguration());
        }
        catch (Exception e) {
            System.out.println("Failed creating stream: '" + STREAM + "' " + e);
        }
    }
}
