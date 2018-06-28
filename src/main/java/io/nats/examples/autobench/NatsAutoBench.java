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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import io.nats.client.Options;

/**
 * Implements a benchmark more like the .net client that loops over a number
 * of scenarios benchmarking each one. This class hardcodes most settings to minimize
 * boilerplate and focus on the nats client code (plus measurement).
 * 
 * Each benchmark is implemented in its own class, and this main class just builds instances
 * and runs them.
 */
public class NatsAutoBench {
    static final String usageString =
    "\nUsage: java NatsAutoBench [server]"
            + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n";

    public static void main(String args[]) {
        String server;

        if (args.length == 1) {
            server = args[0];
        } else if (args.length == 0) {
            server = Options.DEFAULT_URL;
        } else {
            usage();
            return;
        }

        if ("help".equals(server)) {
            usage();
        }

        try {
            Options connectOptions = new Options.Builder().
                                            server(server).
                                            connectionTimeout(Duration.ofMillis(1000)).
                                            noReconnect().
                                            build();
                                       
            List<AutoBenchmark> tests = buildTestList();

            System.out.println("Running warmup");
            runWarmup(connectOptions);

            System.out.printf("Executing %d tests ", tests.size());
            for (AutoBenchmark test : tests) {
                test.execute(connectOptions);
                System.out.print(".");
            }
            System.out.println();
            System.out.println();
            
            Class<? extends AutoBenchmark> testClass = tests.get(0).getClass();
            for (AutoBenchmark test : tests) {

                if (test.getClass() != testClass) {
                    System.out.println();
                    testClass = test.getClass();
                }

                test.printResult();
            }
        } catch (Exception exp) {
            exp.printStackTrace();
        }
    }

    public static void runWarmup(Options connectOptions) throws Exception {
        AutoBenchmark warmup = (new PubSubBenchmark("warmup", 2_000_000, 256));
        warmup.execute(connectOptions);

        if (warmup.getException() != null) {
            System.out.println("Encountered exception "+warmup.getException().getMessage());
            System.exit(-1);
        }
    }

    public static List<AutoBenchmark> buildTestList() {
        ArrayList<AutoBenchmark> tests = new ArrayList<>();
        
        tests.add(new PubBenchmark("PubOnly 0b", 10_000_000, 0));
        tests.add(new PubBenchmark("PubOnly 8b", 10_000_000, 8));
        tests.add(new PubBenchmark("PubOnly 32b", 10_000_000, 32));
        tests.add(new PubBenchmark("PubOnly 256b", 10_000_000, 256));
        tests.add(new PubBenchmark("PubOnly 512b", 10_000_000, 512));
        tests.add(new PubBenchmark("PubOnly 1k", 1_000_000, 1024));
        tests.add(new PubBenchmark("PubOnly 4k", 500_000, 4*1024));
        tests.add(new PubBenchmark("PubOnly 8k", 100_000, 8*1024));

        tests.add(new PubSubBenchmark("PubSub 0b", 10_000_000, 0));
        tests.add(new PubSubBenchmark("PubSub 8b", 10_000_000, 8));
        tests.add(new PubSubBenchmark("PubSub 32b", 10_000_000, 32));
        tests.add(new PubSubBenchmark("PubSub 256b", 10_000_000, 256));
        tests.add(new PubSubBenchmark("PubSub 512b", 5_000_000, 512));
        tests.add(new PubSubBenchmark("PubSub 1k", 1_000_000, 1024));
        tests.add(new PubSubBenchmark("PubSub 4k", 100_000, 4*1024));
        tests.add(new PubSubBenchmark("PubSub 8k", 100_000, 8*1024));

        tests.add(new PubDispatchBenchmark("PubDispatch 0b", 10_000_000, 0));
        tests.add(new PubDispatchBenchmark("PubDispatch 8b", 10_000_000, 8));
        tests.add(new PubDispatchBenchmark("PubDispatch 32b", 10_000_000, 32));
        tests.add(new PubDispatchBenchmark("PubDispatch 256b", 10_000_000, 256));
        tests.add(new PubDispatchBenchmark("PubDispatch 512b", 5_000_000, 512));
        tests.add(new PubDispatchBenchmark("PubDispatch 1k", 1_000_000, 1024));
        tests.add(new PubDispatchBenchmark("PubDispatch 4k", 100_000, 4*1024));
        tests.add(new PubDispatchBenchmark("PubDispatch 8k", 100_000, 8*1024));

        // Request reply is a 4 message trip, so we run fewer
        tests.add(new ReqReplyBenchmark("ReqReply 0b", 20_000, 0));
        tests.add(new ReqReplyBenchmark("ReqReply 8b", 20_000, 8));
        tests.add(new ReqReplyBenchmark("ReqReply 32b", 10_000, 32));
        tests.add(new ReqReplyBenchmark("ReqReply 256b", 10_000, 256));
        tests.add(new ReqReplyBenchmark("ReqReply 512b", 10_000, 512));
        tests.add(new ReqReplyBenchmark("ReqReply 1k", 5_000, 1024));
        tests.add(new ReqReplyBenchmark("ReqReply 4k", 5_000, 4*1024));
        tests.add(new ReqReplyBenchmark("ReqReply 8k", 5_000, 8*1024));

        int latencyTests = 5_000;
        tests.add(new LatencyBenchmark("Latency 0b", latencyTests, 0));
        tests.add(new LatencyBenchmark("Latency 8b", latencyTests, 8));
        tests.add(new LatencyBenchmark("Latency 32b", latencyTests, 32));
        tests.add(new LatencyBenchmark("Latency 256b", latencyTests, 256));
        tests.add(new LatencyBenchmark("Latency 512b", latencyTests, 512));
        tests.add(new LatencyBenchmark("Latency 1k", latencyTests, 1024));
        tests.add(new LatencyBenchmark("Latency 4k", latencyTests, 4 * 1024));
        tests.add(new LatencyBenchmark("Latency 8k", latencyTests, 8 * 1024));

        return tests;
    }

    static void usage() {
        System.err.println(usageString);
        System.exit(-1);
    }
}
