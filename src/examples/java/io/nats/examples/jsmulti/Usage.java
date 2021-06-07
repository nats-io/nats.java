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

public class Usage {

    public static void main(String[] args) {
        usage();
    }

    public static void usage() {
        System.out.println(Usage.USAGE);
    }

    public static final String USAGE =
            "\nUsage: java -cp build/libs/jnats-2.11.5.jar:build/libs/jnats-2.11.5-examples.jar \\"
                    + "\n       io.nats.examples.jsmulti.JsMulti configurations\n"
                    + "\n---------------------------------------------------------------------------------------"
                    + "\nConfiguration Options"
                    + "\n---------------------------------------------------------------------------------------"
                    + "\n-a action (string), required, one of "
                    + "\n   pubSync      - publish synchronously"
                    + "\n   pubAsync     - publish asynchronously"
                    + "\n   pubCore      - core publish (synchronously) to subject"
                    + "\n   subPush      - push subscribe read messages (synchronously)"
                    + "\n   subQueue     - push subscribe read messages with queue (synchronously). "
                    + "\n                    Requires 2 or more threads"
                    + "\n   subPull      - pull subscribe read messages"
                    + "\n   subPullQueue - pull subscribe read messages, queue (using common durable)"
                    + "\n                    Requires 2 or more threads"
                    + "\n---------------------------------------------------------------------------------------"
                    + "\n-s server url (string), optional, defaults to nats://localhost:4222"
                    + "\n-rf report frequency (number) how often to print progress, defaults to 1000 messages."
                    + "\n    <= 0 for no reporting. Reporting time is excluded from timings"
                    + "\n---------------------------------------------------------------------------------------"
                    + "\n-u subject (string), required for publishing or subscribing"
                    + "\n-m message count (number) for publishing or subscribing, defaults to 1 million"
                    + "\n-d threads (number) for publishing or subscribing, defaults to 1"
                    + "\n-n connection strategy (shared|individual) when threading, whether to share"
                    + "\n     the connection, defaults to shared"
                    + "\n-j jitter (number) between publishes or subscribe message retrieval of random"
                    + "\n     number from 0 to j-1, in milliseconds, defaults to 0 (no jitter), maximum 10_000"
                    + "\n     time spent in jitter is excluded from timings"
                    + "\n---------------------------------------------------------------------------------------"
                    + "\n-ps payload size (number) for publishing, defaults to 128, maximum 8192"
                    + "\n-rs round size (number) for pubAsync, default to 100, maximum 1000"
                    + "\n---------------------------------------------------------------------------------------"
                    + "\n-kp ack policy (explicit|none|all) for subscriptions, defaults to explicit"
                    + "\n-kf ack frequency (number), applies to ack policy all, ack after kf messages"
                    + "\n      defaults to 1, maximum 256"
                    + "\n---------------------------------------------------------------------------------------"
                    + "\n-pt pull type (fetch|iterate), fetch all first, or iterate, defaults to iterate."
                    + "\n      Ack policy must be explicit."
                    + "\n-bs batch size (number) for subPull/subPullQueue, defaults to 10, maximum 256"
                    + "\n---------------------------------------------------------------------------------------"
                    + "\nInput numbers can be formatted for easier viewing. For instance, ten thousand"
                    + "\n  can be any of these: 10000 10,000 10.000 10_000"
                    + "\nUse tls:// or opentls:// in the server url to require tls, via the Default SSLContext"
                    + "\n---------------------------------------------------------------------------------------"
            ;
}
