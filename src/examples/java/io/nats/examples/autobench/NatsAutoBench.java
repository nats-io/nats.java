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
    "\nUsage: java NatsAutoBench [serverURL] [help] [utf8] [tiny|small|med]"
    + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
    + "\n\ntiny, small and med reduce the number of messages used for tests, which can help on slower machines\n";

    public static void main(String args[]) {
        String server = Options.DEFAULT_URL;
        boolean utf8 = false;
        int baseMsgs = 100_000;
        int latencyMsgs = 5_000;

        if (args.length > 0) {
            for (String s : args) {
                if (s.equals("utf8")) {
                    utf8 = true;
                }else if (s.equals("med")) {
                    baseMsgs = 50_000;
                    latencyMsgs = 2_500;
                }  else if (s.equals("small")) {
                    baseMsgs = 5_000;
                    latencyMsgs = 250;
                } else if (s.equals("tiny")) {
                    baseMsgs = 1_000;
                    latencyMsgs = 50;
                } else if (s.equals("nano")) {
                    baseMsgs = 10;
                    latencyMsgs = 5;
                } else if (s.equals("help")) {
                    usage();
                    return;
                } else {
                    server = s;
                }
            }
        }

        System.out.printf("Connecting to gnatsd at %s\n", server);

        try {
            Options.Builder builder = new Options.Builder().
                                                    server(server).
                                                    connectionTimeout(Duration.ofSeconds(1)).
                                                    noReconnect();
            
            if (utf8) {
                System.out.printf("Enabling UTF-8 subjects\n");
                builder.supportUTF8Subjects();
            }
                    
            Options connectOptions = builder.build();                   
            List<AutoBenchmark> tests = buildTestList(baseMsgs, latencyMsgs);

            System.out.println("Running warmup");
            runWarmup(connectOptions);

            System.out.printf("Current memory usage is %s / %s / %s free/total/max\n", 
                                AutoBenchmark.humanBytes(Runtime.getRuntime().freeMemory()),
                                AutoBenchmark.humanBytes(Runtime.getRuntime().totalMemory()),
                                AutoBenchmark.humanBytes(Runtime.getRuntime().maxMemory()));
            System.out.print("Executing tests ");
            for (AutoBenchmark test : tests) {
                test.execute(connectOptions);
                System.out.print(".");

                // Ask for GC and wait a moment between tests
                System.gc();
                try {
                    Thread.sleep(500);
                } catch (Exception exp) {
                    // ignore
                }
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

            System.out.println();
            System.out.printf("Final memory usage is %s / %s / %s free/total/max\n", 
                                AutoBenchmark.humanBytes(Runtime.getRuntime().freeMemory()),
                                AutoBenchmark.humanBytes(Runtime.getRuntime().totalMemory()),
                                AutoBenchmark.humanBytes(Runtime.getRuntime().maxMemory()));
        } catch (Exception exp) {
            exp.printStackTrace();
        }

        System.exit(0);
    }

    public static void runWarmup(Options connectOptions) throws Exception {
        AutoBenchmark warmup = (new PubSubBenchmark("warmup", 1_000_000, 64));
        warmup.execute(connectOptions);

        if (warmup.getException() != null) {
            System.out.println("Encountered exception "+warmup.getException().getMessage());
            System.exit(-1);
        }
    }

    public static List<AutoBenchmark> buildTestList(int baseMsgs, int latencyMsgs) {
        ArrayList<AutoBenchmark> tests = new ArrayList<>();
        
        /**/
        tests.add(new PubBenchmark("PubOnly 0b", 100 * baseMsgs, 0));
        tests.add(new PubBenchmark("PubOnly 8b", 100 * baseMsgs, 8));
        tests.add(new PubBenchmark("PubOnly 32b", 100 * baseMsgs, 32));
        tests.add(new PubBenchmark("PubOnly 256b", 100 * baseMsgs, 256));
        tests.add(new PubBenchmark("PubOnly 512b", 100 * baseMsgs, 512));
        tests.add(new PubBenchmark("PubOnly 1k", 10 * baseMsgs, 1024));
        tests.add(new PubBenchmark("PubOnly 4k", 5 * baseMsgs, 4*1024));
        tests.add(new PubBenchmark("PubOnly 8k", baseMsgs, 8*1024));

        tests.add(new PubSubBenchmark("PubSub 0b", 100 * baseMsgs, 0));
        tests.add(new PubSubBenchmark("PubSub 8b", 100 * baseMsgs, 8));
        tests.add(new PubSubBenchmark("PubSub 32b", 100 * baseMsgs, 32));
        tests.add(new PubSubBenchmark("PubSub 256b", 100 * baseMsgs, 256));
        tests.add(new PubSubBenchmark("PubSub 512b", 50 * baseMsgs, 512));
        tests.add(new PubSubBenchmark("PubSub 1k", 10 * baseMsgs, 1024));
        tests.add(new PubSubBenchmark("PubSub 4k", baseMsgs, 4*1024));
        tests.add(new PubSubBenchmark("PubSub 8k", baseMsgs, 8*1024));
        
        tests.add(new PubDispatchBenchmark("PubDispatch 0b", 100 * baseMsgs, 0));
        tests.add(new PubDispatchBenchmark("PubDispatch 8b", 100 * baseMsgs, 8));
        tests.add(new PubDispatchBenchmark("PubDispatch 32b", 100 * baseMsgs, 32));
        tests.add(new PubDispatchBenchmark("PubDispatch 256b", 100 * baseMsgs, 256));
        tests.add(new PubDispatchBenchmark("PubDispatch 512b", 50 * baseMsgs, 512));
        tests.add(new PubDispatchBenchmark("PubDispatch 1k", 10 * baseMsgs, 1024));
        tests.add(new PubDispatchBenchmark("PubDispatch 4k", baseMsgs, 4*1024));
        tests.add(new PubDispatchBenchmark("PubDispatch 8k", baseMsgs, 8*1024));
        
        // Request reply is a 4 message trip, and runs the full loop before sending another message
        // so we run fewer because the client cannot batch any socket calls to the server together
        tests.add(new ReqReplyBenchmark("ReqReply 0b", baseMsgs / 5, 0));
        tests.add(new ReqReplyBenchmark("ReqReply 8b", baseMsgs / 5, 8));
        tests.add(new ReqReplyBenchmark("ReqReply 32b", baseMsgs / 10, 32));
        tests.add(new ReqReplyBenchmark("ReqReply 256b", baseMsgs / 10, 256));
        tests.add(new ReqReplyBenchmark("ReqReply 512b", baseMsgs / 10, 512));
        tests.add(new ReqReplyBenchmark("ReqReply 1k", baseMsgs / 10, 1024));
        tests.add(new ReqReplyBenchmark("ReqReply 4k", baseMsgs / 10, 4*1024));
        tests.add(new ReqReplyBenchmark("ReqReply 8k", baseMsgs / 10, 8*1024));

        tests.add(new LatencyBenchmark("Latency 0b", latencyMsgs, 0));
        tests.add(new LatencyBenchmark("Latency 8b", latencyMsgs, 8));
        tests.add(new LatencyBenchmark("Latency 32b", latencyMsgs, 32));
        tests.add(new LatencyBenchmark("Latency 256b", latencyMsgs, 256));
        tests.add(new LatencyBenchmark("Latency 512b", latencyMsgs, 512));
        tests.add(new LatencyBenchmark("Latency 1k", latencyMsgs, 1024));
        tests.add(new LatencyBenchmark("Latency 4k", latencyMsgs, 4 * 1024));
        tests.add(new LatencyBenchmark("Latency 8k", latencyMsgs, 8 * 1024));
        /**/

        return tests;
    }

    static void usage() {
        System.err.println(usageString);
        System.exit(-1);
    }
}
