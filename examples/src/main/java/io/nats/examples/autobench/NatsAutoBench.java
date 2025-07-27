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

import io.nats.client.Options;

import javax.net.ssl.SSLContext;
import java.security.Provider;
import java.security.Security;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implements a benchmark more like the .net client that loops over a number
 * of scenarios benchmarking each one. This class hard codes most settings to minimize
 * boilerplate and focus on the nats client code (plus measurement).
 * 
 * Each benchmark is implemented in its own class, and this main class just builds instances
 * and runs them.
 */
public class NatsAutoBench {
    static final String usageString =
            "\nUsage: java -cp <classpath> NatsAutoBench" +
                    "\n[serverURL] [help] [tiny|small|med|large] [conscrypt] [jsfile]" +
                    "\n[PubOnly] [PubOnlyWithHeaders] [PubSub] [PubDispatch] [ReqReply] [Latency] " +
                    "\n[JsPubSync] [JsPubAsync] [JsSub] [JsPubRounds]" +
                    "[-lcsv <filespec>] \n\n"
            + "If no specific test name(s) are supplied all will be run, otherwise only supplied tests will be run."
            + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
            + "\n\ntiny, small and med reduce the number of messages used for tests, which can help on slower machines\n";

    public static void main(String[] args) {

        // TO RUN WITH ARGS FROM IDE, ADD A LINE LIKE THESE
        // args = "myhost:4222 med".split(" ");
         args = "small PubOnly".split(" ");
        // args = "large PubOnlyWithHeaders".split(" ");
        // args = "med JsPubAsync".split(" ");
        // args = "help".split(" ");
        // args = "latency large".split(" ");

        Arguments a = readArgs(args);

        System.out.printf("Connecting to NATS server at %s\n", a.server);

        try {
            Options.Builder builder = new Options.Builder().
                                                    server(a.server).
                                                    connectionTimeout(Duration.ofSeconds(1)).
                                                    noReconnect();

            /**
             * The conscrypt flag is provided for testing with the conscrypt jar. Using it through reflection is
             * deprecated but allows the library to ship without a dependency. Using conscrypt should only require the
             * jar plus the flag. For example, to run after building locally and using the test cert files:
             * java -cp ./build/libs/jnats-&lt;version&gt;-examples.jar:./build/libs/jnats-&lt;version&gt;-fat.jar:<path to conscrypt.jar> \
             * -Djavax.net.ssl.keyStore=src/test/resources/keystore.jks -Djavax.net.ssl.keyStorePassword=password \
             * -Djavax.net.ssl.trustStore=src/test/resources/truststore.jks -Djavax.net.ssl.trustStorePassword=password \
             * io.nats.examples.autobench.NatsAutoBench tls://localhost:4443 med conscrypt
             */
            if (a.conscrypt) {
                Provider provider = (Provider) Class.forName("org.conscrypt.OpenSSLProvider").newInstance();
                Security.insertProviderAt(provider, 1);
            }

            if (a.server.startsWith("tls")) {
                System.out.println("Security Provider - "+ SSLContext.getDefault().getProvider().getInfo());
            }
                    
            Options connectOptions = builder.build();                   
            List<AutoBenchmark> tests = buildTestList(a);

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
            
            Class<? extends AutoBenchmark> lastTestClass = null;
            AutoBenchmark lastTest = null;
            for (AutoBenchmark test : tests) {
                if (test.getClass() != lastTestClass) {
                    if (lastTestClass != null) {
                        test.afterPrintLastOfKind();
                        System.out.println();
                    }
                    test.beforePrintFirstOfKind();
                    lastTest = test;
                    lastTestClass = test.getClass();
                }

                test.printResult();
            }
            if (lastTest != null) {
                lastTest.afterPrintLastOfKind();
            }

            System.out.println();
            System.out.printf("Final memory usage is %s / %s / %s free/total/max\n", 
                                AutoBenchmark.humanBytes(Runtime.getRuntime().freeMemory()),
                                AutoBenchmark.humanBytes(Runtime.getRuntime().totalMemory()),
                                AutoBenchmark.humanBytes(Runtime.getRuntime().maxMemory()));
        } catch (Exception exp) {
            exp.printStackTrace();
        }
    }

    public static void runWarmup(Options connectOptions) throws Exception {
        AutoBenchmark warmup = (new PubSubBenchmark("warmup", 100_000, 64));
        warmup.execute(connectOptions);

        if (warmup.getException() != null) {
            System.out.println("Encountered exception "+warmup.getException().getMessage());
            System.exit(-1);
        }
    }

    public static List<AutoBenchmark> buildTestList(Arguments a) {
        List<AutoBenchmark> tests = new ArrayList<>();

        if (a.allTests || a.pubOnly) {
            addTests(a.baseMsgs, a.maxSize, tests, sizes, msgsMultiple,
                (msize, mcnt) -> new PubBenchmark("PubOnly " + msize, mcnt, msize));
        }

        if (a.PubOnlyWithHeaders) {
            addTests(a.baseMsgs, a.maxSize, tests, sizes, msgsMultiple,
                (msize, mcnt) -> new PubWithHeadersBenchmark("PubOnlyWithHeaders " + msize, mcnt, msize));
        }

        if (a.allTests || a.pubSub) {
            addTests(a.baseMsgs, a.maxSize, tests, sizes, msgsMultiple,
                    (msize, mcnt) -> new PubSubBenchmark("PubSub " + msize, mcnt, msize));
        }

        if (a.allTests || a.pubDispatch) {
            addTests(a.baseMsgs, a.maxSize, tests, sizes, msgsMultiple,
                    (msize, mcnt) -> new PubDispatchBenchmark("PubDispatch " + msize, mcnt, msize));
        }

        AtomicBoolean jsPubSyncSaveForJsSub = new AtomicBoolean(false);
        AtomicBoolean jsPubAsyncSaveForJsSub = new AtomicBoolean(a.allTests);

        if (!a.allTests && a.jsSub) {
            if (a.jsPubAsync || !a.jsPubSync) {
                a.jsPubAsync = true; // have to publish somewhere, async is faster
                jsPubAsyncSaveForJsSub.set(true);
            }
            else {
                jsPubSyncSaveForJsSub.set(false);
            }
        }

        if (a.allTests || a.jsPubSync) {
            addTests(a.baseMsgs, a.maxSize, tests, sizes, msgsMultiple,
                    (msize, mcnt) -> new JsPubBenchmark("JsPubSync " + msize, mcnt, msize, a.jsFile, true, jsPubSyncSaveForJsSub.get()));
        }

        if (a.allTests || a.jsPubAsync) {
            addTests(a.baseMsgs, a.maxSize, tests, sizes, msgsMultiple,
                    (msize, mcnt) -> new JsPubBenchmark("JsPubAsync " + msize, mcnt, msize, a.jsFile, false, jsPubAsyncSaveForJsSub.get()));
        }

        if (a.allTests || a.jsSub) {
            addTests(a.baseMsgs, a.maxSize, tests, sizes, msgsMultiple,
                    (msize, mcnt) -> new JsSubBenchmark("JsSub " + msize, mcnt, msize));
        }

        if (a.allTests || a.jsPubRounds) {
            addTestsWithRounds(a.baseMsgs, a.maxSize, tests, sizes, msgsMultiple,
                    (msize, mcnt, rsize) -> new JsPubAsyncRoundsBenchmark("JsPubAsyncRounds " + msize + "," + rsize, mcnt, msize, a.jsFile, rsize));
        }


        if (a.allTests || a.reqReply) {
                addRequestReplyTests(a.baseMsgs, a.maxSize, tests, sizes, msgsDivider,
                    (msize, mcnt) -> new ReqReplyBenchmark("ReqReply " + msize, mcnt, msize));
        }

        if (a.allTests || a.latency) {
                addLatencyTests(a.latencyMsgs, a.maxSize, tests, sizes,
                    (msize, mcnt) -> new LatencyBenchmark("Latency " + msize, mcnt, msize, a.lcsv));
        }

        return tests;
    }

    private static void addTests(int baseMsgs, long maxSize, List<AutoBenchmark> tests, int[] sizes, long[] msgsMultiple, AutoBenchmarkConstructor abc) {
        for(int i = 0; i< sizes.length; i++) {
            int size = sizes[i];
            long msgMult = msgsMultiple[i];

            if (size > maxSize) {
                break;
            }

            tests.add(abc.construct(size, msgMult * baseMsgs));
        }
    }

    private static void addLatencyTests(int latencyMsgs, long maxSize, List<AutoBenchmark> tests, int[] sizes, AutoBenchmarkConstructor abc) {
        for (int size : sizes) {
            if (size > maxSize) {
                break;
            }
            tests.add(abc.construct(size, latencyMsgs));
        }
    }

    // Request reply is a 4 message trip, and runs the full loop before sending another message
    // so we run fewer because the client cannot batch any socket calls to the server together
    private static void addRequestReplyTests(int baseMsgs, long maxSize, List<AutoBenchmark> tests, int[] sizes, int[] msgsDivider, AutoBenchmarkConstructor abc) {
        for(int i = 0; i< sizes.length; i++) {
            int size = sizes[i];
            int msgDivide = msgsDivider[i];

            if(size > maxSize) {
                break;
            }

            tests.add(abc.construct(size, baseMsgs / msgDivide));
        }
    }

    private static void addTestsWithRounds(int baseMsgs, long maxSize, List<AutoBenchmark> tests, int[] sizes, long[] msgsMultiple, AutoBenchmarkRoundSizeConstructor abrsc) {
        for(int i = 0; i< sizes.length; i++) {
            int size = sizes[i];
            long msgMult = msgsMultiple[i];

            if (size > maxSize) {
                break;
            }

            for (int rs : roundSize) {
                if (rs <= msgMult * baseMsgs) {
                    tests.add(abrsc.construct(size, msgMult * baseMsgs, rs));
                }
            }
        }
    }

    static int[] sizes =         {0,     8,  32, 256, 512, 1024, 4096, 8192};
    static long[] msgsMultiple = {100, 100, 100, 100, 100,   10,    5,    1};
    static int[] msgsDivider =   {5,     5,  10,  10,  10,   10,   10,   10};
    static int[] roundSize =     {10, 100, 200, 500, 1000};

    interface AutoBenchmarkConstructor {
        AutoBenchmark construct(long messageSize, long messageCount);
    }

    interface AutoBenchmarkRoundSizeConstructor {
        AutoBenchmark construct(long messageSize, long messageCount, int roundSize);
    }

    static class Arguments {
        String server = Options.DEFAULT_URL;
        boolean conscrypt = false;
        int baseMsgs = 100_000;
        int latencyMsgs = 5_000;
        long maxSize = 8192;
        boolean allTests = true;

        boolean pubOnly = false;
        boolean PubOnlyWithHeaders = false;
        boolean pubSub = false;
        boolean pubDispatch = false;
        boolean reqReply = false;
        boolean latency = false;
        boolean jsPubSync = false;
        boolean jsPubAsync = false;
        boolean jsSub = false;
        boolean jsPubRounds = false;
        boolean jsFile = false;
        String lcsv = null;
    }

    private static Arguments readArgs(String[] args) {
        Arguments a = new Arguments();
        if (args.length > 0) {
            for (int x = 0; x < args.length; x++) {
                switch (args[x].toLowerCase()) {
                    case "conscrypt":
                        a.conscrypt = true;
                        break;
                    case "large":
                        a.baseMsgs = 500_000;
                        a.latencyMsgs = 25_000;
                        break;
                    case "med":
                        a.baseMsgs = 50_000;
                        a.latencyMsgs = 2_500;
                        break;
                    case "small":
                        a.baseMsgs = 5_000;
                        a.latencyMsgs = 250;
                        a.maxSize = 1024;
                        break;
                    case "tiny":
                        a.baseMsgs = 1_000;
                        a.latencyMsgs = 50;
                        a.maxSize = 1024;
                        break;
                    case "nano":
                        a.baseMsgs = 10;
                        a.latencyMsgs = 5;
                        a.maxSize = 512;
                        break;
                    case "pubonly":
                        a.allTests = false;
                        a.pubOnly = true;
                        break;
                    case "pubonlywithheaders":
                        a.allTests = false;
                        a.PubOnlyWithHeaders = true;
                        break;
                    case "pubsub":
                        a.allTests = false;
                        a.pubSub = true;
                        break;
                    case "pubdispatch":
                        a.allTests = false;
                        a.pubDispatch = true;
                        break;
                    case "reqreply":
                        a.allTests = false;
                        a.reqReply = true;
                        break;
                    case "latency":
                        a.allTests = false;
                        a.latency = true;
                        break;
                    case "jspubsync":
                        a.allTests = false;
                        a.jsPubSync = true;
                        break;
                    case "jspubasync":
                        a.allTests = false;
                        a.jsPubAsync = true;
                        break;
                    case "jssub":
                        a.allTests = false;
                        a.jsSub = true;
                        break;
                    case "jspubrounds":
                        a.allTests = false;
                        a.jsPubRounds = true;
                        break;
                    case "jsfile":
                        a.jsFile = true;
                        break;
                    case "-lcsv":
                        a.lcsv = args[++x];
                        break;
                    case "help":
                        System.err.println(usageString);
                        System.exit(-1);
                    default:
                        a.server = args[x];
                        break;
                }
            }
        }
        return a;
    }
}
