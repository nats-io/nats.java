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

import java.security.Provider;
import java.security.Security;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLContext;

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
        boolean conscrypt = false;
        int baseMsgs = 100_000;
        int latencyMsgs = 5_000;
        long maxSize = 8*1024;

        if (args.length > 0) {
            for (String s : args) {
                if (s.equals("utf8")) {
                    utf8 = true;
                } else if (s.equals("conscrypt")) {
                    conscrypt = true;
                } else if (s.equals("med")) {
                    baseMsgs = 50_000;
                    latencyMsgs = 2_500;
                } else if (s.equals("small")) {
                    baseMsgs = 5_000;
                    latencyMsgs = 250;
                    maxSize = 1024;
                } else if (s.equals("tiny")) {
                    baseMsgs = 1_000;
                    latencyMsgs = 50;
                    maxSize = 1024;
                } else if (s.equals("nano")) {
                    baseMsgs = 10;
                    latencyMsgs = 5;
                    maxSize = 512;
                } else if (s.equals("help")) {
                    usage();
                    return;
                } else {
                    server = s;
                }
            }
        }

        System.out.printf("Connecting to NATS server at %s\n", server);

        try {
            Options.Builder builder = new Options.Builder().
                                                    server(server).
                                                    connectionTimeout(Duration.ofSeconds(1)).
                                                    noReconnect();
            
            if (utf8) {
                System.out.printf("Enabling UTF-8 subjects\n");
                builder.supportUTF8Subjects();
            }
            
            /**
             * The conscrypt flag is provided for testing with the conscrypt jar. Using it through reflection is
             * deprecated but allows the library to ship without a dependency. Using conscrypt should only require the
             * jar plus the flag. For example, to run after building locally and using the test cert files:
             * java -cp ./build/libs/jnats-2.5.1-SNAPSHOT-examples.jar:./build/libs/jnats-2.5.1-SNAPSHOT-fat.jar:<path to conscrypt.jar> \
             * -Djavax.net.ssl.keyStore=src/test/resources/keystore.jks -Djavax.net.ssl.keyStorePassword=password \
             * -Djavax.net.ssl.trustStore=src/test/resources/cacerts -Djavax.net.ssl.trustStorePassword=password \
             * io.nats.examples.autobench.NatsAutoBench tls://localhost:4443 med conscrypt
             */
            if (conscrypt) {
                Provider provider = (Provider) Class.forName("org.conscrypt.OpenSSLProvider").newInstance();
                Security.insertProviderAt(provider, 1);
            }

            if (server.startsWith("tls")) {
                System.out.println("Security Provider - "+ SSLContext.getDefault().getProvider().getInfo());
            }
                    
            Options connectOptions = builder.build();                   
            List<AutoBenchmark> tests = buildTestList(baseMsgs, latencyMsgs, maxSize);

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
    }

    public static void runWarmup(Options connectOptions) throws Exception {
        AutoBenchmark warmup = (new PubSubBenchmark("warmup", 100_000, 64));
        warmup.execute(connectOptions);

        if (warmup.getException() != null) {
            System.out.println("Encountered exception "+warmup.getException().getMessage());
            System.exit(-1);
        }
    }

    public static List<AutoBenchmark> buildTestList(int baseMsgs, int latencyMsgs, long maxSize) {
        ArrayList<AutoBenchmark> tests = new ArrayList<>();
        
        int[] sizes = {0, 8, 32, 256, 512, 1024, 4*1024, 8*1024};
        int[] msgsMultiple = {100, 100, 100, 100, 100, 10, 5, 1};
        int[] msgsDivider = {5, 5, 10, 10, 10, 10, 10, 10};

        /**/
        for(int i=0; i<sizes.length; i++) {
            int size = sizes[i];
            int msgMult = msgsMultiple[i];

            if(size > maxSize) {
                break;
            }

            tests.add(new PubBenchmark("PubOnly "+size, msgMult * baseMsgs, size));
        }

        for(int i=0; i<sizes.length; i++) {
            int size = sizes[i];
            int msgMult = msgsMultiple[i];

            if(size > maxSize) {
                break;
            }

            tests.add(new PubSubBenchmark("PubSub "+size, msgMult * baseMsgs, size));
        }

        for(int i=0; i<sizes.length; i++) {
            int size = sizes[i];
            int msgMult = msgsMultiple[i];

            if(size > maxSize) {
                break;
            }

            tests.add(new PubDispatchBenchmark("PubDispatch "+size, msgMult * baseMsgs, size));
        }

        // Request reply is a 4 message trip, and runs the full loop before sending another message
        // so we run fewer because the client cannot batch any socket calls to the server together
        for(int i=0; i<sizes.length; i++) {
            int size = sizes[i];
            int msgDivide = msgsDivider[i];

            if(size > maxSize) {
                break;
            }

            tests.add(new ReqReplyBenchmark("ReqReply "+size, baseMsgs / msgDivide, size));
        }

        for(int i=0; i<sizes.length; i++) {
            int size = sizes[i];

            if(size > maxSize) {
                break;
            }

            tests.add(new LatencyBenchmark("Latency "+size, latencyMsgs, size));
        }
        /**/

        return tests;
    }

    static void usage() {
        System.err.println(usageString);
        System.exit(-1);
    }
}
