// Copyright 2023 The NATS Authors
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
import io.nats.client.Options;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.examples.testapp.support.CommandLine;
import io.nats.examples.testapp.support.CommandLineConsumer;
import io.nats.examples.testapp.support.ConsumerType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class App {

    public static String[] MANUAL_ARGS = (
//        "--servers nats://192.168.50.99:4222"
        " --servers nats://localhost:4222"
            + " --stream app-stream"
            + " --subject app-subject"
            + " --subject app-subject"
//            + " --runtime 60" // minutes
            + " --pubjitter 50"
//            + " --simple ordered,100,5000"
//            + " --simple durable 100 5000"
            + " --push ordered"
//            + " --push durable"
            + " --screen left"
    ).split(" ");

    public static void main(String[] args) throws Exception {

        args = MANUAL_ARGS; // comment in to use

        CommandLine cmd = new CommandLine(args);

        try {
            Ui.start(cmd.uiScreen, cmd.work, cmd.debug);
            Ui.controlMessage("APP", cmd.toString().replace(" --", "    \n--"));

            Options options = Options.builder().servers(cmd.servers).build();
            try (Connection nc = Nats.connect(options)) {
                JetStreamManagement jsm = nc.jetStreamManagement();
                createOrReplaceStream(cmd, jsm);
            }
            catch (Exception e) {
                Ui.controlMessage("APP", e.getMessage());
            }

            Publisher publisher = new Publisher(cmd, cmd.pubjitter);
            Thread pubThread = new Thread(publisher);
            pubThread.start();

            CountDownLatch waiter = new CountDownLatch(1);

            List<ConnectableConsumer> cons = new ArrayList<>();
            for (CommandLineConsumer clc : cmd.commandLineConsumers) {
                ConnectableConsumer con;
                if (clc.consumerType == ConsumerType.Simple) {
                    con = new SimpleConsumer(cmd, clc.consumerKind, clc.batchSize, clc.expiresIn);
                }
                else {
                    con = new PushConsumer(cmd, clc.consumerKind);
                }
                Ui.consoleMessage("APP", con.label);
                cons.add(con);
            }

            Monitor monitor = new Monitor(cmd, publisher, cons);
            Thread monThread = new Thread(monitor);
            monThread.start();

            //noinspection ResultOfMethodCallIgnored
            long runtime = cmd.runtime < 1 ? Long.MAX_VALUE : cmd.runtime;
            waiter.await(runtime, TimeUnit.MILLISECONDS);
        }
        catch (Exception e) {
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
        }
        Ui.dumpControl();
        System.exit(0);
    }

    public static void createOrReplaceStream(CommandLine cmd, JetStreamManagement jsm) {
        try {
            jsm.deleteStream(cmd.stream);
        }
        catch (Exception ignore) {}
        try {
            StreamConfiguration sc = StreamConfiguration.builder()
                .name(cmd.stream)
                .storageType(StorageType.File)
                .subjects(cmd.subject)
                .build();
            StreamInfo si = jsm.addStream(sc);
            Ui.controlMessage("APP", si.getConfiguration());
        }
        catch (Exception e) {
            Ui.consoleMessage("FATAL", "Failed creating stream: '" + cmd.stream + "' " + e);
            System.exit(-1);
        }
    }
}
