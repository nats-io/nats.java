// Copyright 2021-2022 The NATS Authors
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

    public final String action;
    public final String latencyAction;
    public final boolean latencyTracking;
    public final String server;
    public final String optionsFactoryClassName;
    public final int reportFrequency;
    public final String subject;
    public final int messageCount;
    public final int threads;
    public final boolean connShared;
    public final long jitter;
    public final int payloadSize;
    public final int roundSize;
    public final boolean pullTypeIterate;
    public final AckPolicy ackPolicy;
    public final int ackFrequency;
    public final int batchSize;
    public final String queueDurable = "qd" + uniqueEnough();
    public final String queueName = "qn" + uniqueEnough();

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
        String lAction = latencyAction == null ? "" : "\n  latency action (-la)      " + latencyAction;
        String lTracking = latencyAction == null && latencyTracking ? "\n  latency tracking (-lt)    On" : "";

        String srv = optionsFactoryClassName == null
            ? "\n  server (-s):              " + (server == null ? Options.DEFAULT_URL : server)
            : "\n  options factory (-of)     " + optionsFactoryClassName;

        String s = "JetStream Multi-Tool Run Config:"
                + "\n  action (-a):              " + action
                + lAction + lTracking + srv
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

        if (contains(SUB_ACTIONS, action) || contains(SUB_ACTIONS, latencyAction)) {
            s += "\n  ack policy (-kp):         " + ackPolicy;
            s += "\n  ack frequency (-kf):      " + ackFrequency;
        }

        if (contains(PULL_ACTIONS, action) || contains(PULL_ACTIONS, latencyAction)) {
            s += "\n  pull type (-pt):          " + (pullTypeIterate ? ITERATE : FETCH);
            s += "\n  batch size (-b):          " + batchSize;
        }

        return s;
    }

    public Arguments(String[] args) {
        if (args == null || args.length == 0) {
            exit();
        }

        String _action = null;
        String _latencyAction = null;
        boolean _latencyHeaders = false;
        String _server = Options.DEFAULT_URL;
        String _optionsFactoryClassName = null;
        int _reportFrequency = 1000;
        String _subject = "sub" + uniqueEnough();
        int _messageCount = 1_000_000;
        int _threads = 1;
        boolean _connShared = true;
        long _jitter = 0;
        int _payloadSize = 128;
        int _roundSize = 100;
        boolean _pullTypeIterate = true;
        AckPolicy _ackPolicy = AckPolicy.Explicit;
        int _ackFrequency = 1;
        int _batchSize = 10;

        if (args != null && args.length > 0) {

            for (int x = 0; x < args.length; x++) {
                String arg = args[x].trim();
                switch (arg) {
                    case "-s":
                        _server = asString(args, ++x);
                        break;
                    case "-of":
                        _optionsFactoryClassName = asString(args, ++x);
                        break;
                    case "-a":
                        _action = asString(args, ++x).toLowerCase();
                        break;
                    case "-la":
                        _latencyAction = asString(args, ++x).toLowerCase();
                        _latencyHeaders = true;
                        break;
                    case "-lt":
                        _latencyHeaders = true;
                        break;
                    case "-u":
                        _subject = asString(args, ++x);
                        break;
                    case "-m":
                        _messageCount = asNumber(args, ++x, -1, "total messages");
                        break;
                    case "-ps":
                        _payloadSize = asNumber(args, ++x, 1048576, "payload size");
                        break;
                    case "-bs":
                        _batchSize = asNumber(args, ++x, 256, "batch size");
                        break;
                    case "-rs":
                        _roundSize = asNumber(args, ++x, 1000, "round size");
                        break;
                    case "-d":
                        _threads = asNumber(args, ++x, 10, "number of threads");
                        break;
                    case "-j":
                        _jitter = asNumber(args, ++x, 10_000, "jitter");
                        break;
                    case "-n":
                        _connShared = trueIfNot(args, ++x, "individual");
                        break;
                    case "-kp":
                        _ackPolicy = AckPolicy.get(asString(args, ++x).toLowerCase());
                        if (_ackPolicy == null) {
                            _ackPolicy = AckPolicy.Explicit;
                        }
                        break;
                    case "-kf":
                        _ackFrequency = asNumber(args, ++x, 256, "ack frequency");
                        break;
                    case "-pt":
                        _pullTypeIterate = trueIfNot(args, ++x, "fetch");
                        break;
                    case "-rf":
                        _reportFrequency = asNumber(args, ++x, -2, "report frequency");
                        break;
                    case "":
                        break;
                    default:
                        error("Unknown argument: " + arg);
                        break;
                }
            }
        }

        if (!contains(ALL_ACTIONS, _action)) {
            error("Valid action required!");
        }

        if (_latencyAction != null) {
            if (!contains(SUB_ACTIONS, _latencyAction)) {
                error("Valid subscribe action required for latency!");
            }
            if (!contains(PUB_ACTIONS, _action)) {
                error("Valid publish action required for latency!");
            }
        }

        if (_threads < 2 && (contains(QUEUE_ACTIONS, _action) || contains(QUEUE_ACTIONS, _latencyAction))) {
            error("Queue subscribing requires multiple threads!");
        }

        if (_optionsFactoryClassName == null && !_server.equals(Options.DEFAULT_URL)) {
            try {
                new Options.Builder().build().createURIForServer(_server);
            } catch (URISyntaxException e) {
                error("Invalid server URI: " + _server);
            }
        }

        action = _action;
        latencyAction = _latencyAction;
        latencyTracking = _latencyHeaders;
        server = _server;
        optionsFactoryClassName = _optionsFactoryClassName;
        reportFrequency = _reportFrequency;
        subject = _subject;
        messageCount = _messageCount;
        threads = _threads;
        connShared = _connShared;
        jitter = _jitter;
        payloadSize = _payloadSize;
        roundSize = _roundSize;
        pullTypeIterate = _pullTypeIterate;
        ackPolicy = _ackPolicy;
        ackFrequency = _ackFrequency;
        batchSize = _batchSize;
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
        return action != null && Arrays.asList(list.split(" ")).contains(action);
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
