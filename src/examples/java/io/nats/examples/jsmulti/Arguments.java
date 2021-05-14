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

package io.nats.examples.jsmulti;

import io.nats.client.Options;
import io.nats.client.api.AckPolicy;

import java.net.URISyntaxException;
import java.util.Arrays;

import static io.nats.examples.ExampleUtils.uniqueEnough;
import static io.nats.examples.jsmulti.Constants.*;

// ----------------------------------------------------------------------------------------------------
// Arguments
// ----------------------------------------------------------------------------------------------------
class Arguments{

    String action;
    String server = Options.DEFAULT_URL;
    int reportFrequency = 1000;
    String subject;
    int messageCount = 1_000_000;
    int threads = 1;
    boolean connShared = true;
    long jitter = 0;
    int payloadSize = 128;
    int roundSize = 100;
    boolean pullTypeIterate = true;
    AckPolicy ackPolicy = AckPolicy.Explicit;
    int ackFrequency = 1;
    int batchSize = 10;
    final String queueDurable = "qd" + uniqueEnough();
    final String queueName = "qn" + uniqueEnough();

    private byte[] _payload;
    byte[] getPayload() {
        if (_payload == null) {
            _payload = new byte[payloadSize];
        }
        return _payload;
    }

    public int perThread() {
        return messageCount / threads;
    }

    public void print() {
        System.out.println(this + "\n");
    }

    @Override
    public String toString() {
        String s = "JetStream Multi-Tool Run Config:"
                + "\n  action (-a):              " + action
                + "\n  server (-s):              " + (server == null ? Options.DEFAULT_URL : server)
                + "\n  report frequency (-rf):   " + (reportFrequency == Integer.MAX_VALUE ? "no reporting" : "" + reportFrequency)
                + "\n  subject (-u):             " + subject
                + "\n  message count (-m):       " + messageCount
                + "\n  threads (-d):             " + threads
                + "\n  jitter (-j):              " + jitter;

        if (threads > 1) {
            s += "\n  connection strategy (-n): " + (connShared ? SHARED : INDIVIDUAL);
        }

        if (contains(PUB_ACTIONS, action)) {
            s += "\n  payload size (-p):        " + payloadSize + " bytes";
        }

        if (PUB_ASYNC.equals(action)) {
            s += "\n  round size (-r):          " + roundSize;
        }

        if (contains(SUB_ACTIONS, action)) {
            s += "\n  ack policy (-kp):         " + ackPolicy;
            s += "\n  ack frequency (-kf):      " + ackFrequency;
        }

        if (contains(PULL_ACTIONS, action)) {
            s += "\n  pull type (-pt):          " + (pullTypeIterate ? ITERATE : FETCH);
            s += "\n  batch size (-b):          " + batchSize;
        }
        return s;
    }

    public Arguments(String[] args) {
        if (args == null || args.length == 0) {
            exit();
        }

        if (args != null && args.length > 0) {
            for (int x = 0; x < args.length; x++) {
                String arg = args[x].trim();
                switch (arg) {
                    case "-s":
                        server = asString(args, ++x);
                        break;
                    case "-a":
                        action = asString(args, ++x).toLowerCase();
                        break;
                    case "-u":
                        subject = asString(args, ++x);
                        break;
                    case "-m":
                        messageCount = asNumber(args, ++x, -1, "total messages");
                        break;
                    case "-ps":
                        payloadSize = asNumber(args, ++x, 8192, "payload size");
                        break;
                    case "-bs":
                        batchSize = asNumber(args, ++x, 256, "batch size");
                        break;
                    case "-rs":
                        roundSize = asNumber(args, ++x, 1000, "round size");
                        break;
                    case "-d":
                        threads = asNumber(args, ++x, 10, "number of threads");
                        break;
                    case "-j":
                        jitter = asNumber(args, ++x, 10_000, "jitter");
                        break;
                    case "-n":
                        connShared = trueIfNot(args, ++x, "individual");
                        break;
                    case "-kp":
                        ackPolicy = AckPolicy.get(asString(args, ++x).toLowerCase());
                        if (ackPolicy == null) {
                            ackPolicy = AckPolicy.Explicit;
                        }
                        break;
                    case "-kf":
                        ackFrequency = asNumber(args, ++x, 256, "ack frequency");
                        break;
                    case "-pt":
                        pullTypeIterate = trueIfNot(args, ++x, "fetch");
                        break;
                    case "-rf":
                        reportFrequency = asNumber(args, ++x, -2, "report frequency");
                        break;
                    case "":
                        break;
                    default:
                        error("Unknown argument: " + arg);
                        break;
                }
            }
        }

        if (action == null || !contains(ALL_ACTIONS, action)) {
            error("Valid action required!");
        }

        if (subject == null) {
            error("Publish or Subscribe actions require subject name!");
        }

        if (threads < 2 && contains(QUEUE_ACTIONS, action)) {
            error("Queue subscribing requires multiple threads!");
        }

        try {
            new Options.Builder().build().createURIForServer(server);
        } catch (URISyntaxException e) {
            error("Invalid server URI: " + server);
        }
    }

    private static void error(String errMsg) {
        System.err.println("\nERROR: " + errMsg);
        exit();
    }

    private static void exit() {
        System.err.println(Usage.USAGE);
        System.exit(-1);
    }

    private static boolean contains(String list, String action) {
        return Arrays.asList(list.split(" ")).contains(action);
    }

    private static String normalize(String s) {
        return s.replaceAll("_", "").replaceAll(",", "").replaceAll("\\.", "");
    }

    private static int asNumber(String[] args, int index, int upper, String label) {
        int v = Integer.parseInt(normalize(asString(args, index)));
        if (upper == -2 && v < 1) {
            return Integer.MAX_VALUE;
        }
        if (upper > 0) {
            if (v > upper) {
                Arguments.error("value for " + label + " cannot exceed " + upper);
            }
        }
        return v;
    }

    private static String asString(String[] args, int index) {
        return args[index].trim();
    }

    private static boolean trueIfNot(String[] args, int index, String notDefault) {
        return !asString(args, index).equalsIgnoreCase(notDefault);
    }
}
