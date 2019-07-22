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

package io.nats.examples;

import java.time.Duration;

import io.nats.client.AuthHandler;
import io.nats.client.Connection;
import io.nats.client.Consumer;
import io.nats.client.ErrorListener;
import io.nats.client.Nats;
import io.nats.client.Options;

public class ConnectTime {
    static final String usageString =
            "\nUsage: java ConnectTime server\n"
            + "\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
            + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
            + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
            + "\nUse the URL for user/pass/token authentication.\n";
    
    public static Options createOptions(String server) throws Exception {
        Options.Builder builder = new Options.Builder().
                        server(server).
                        connectionTimeout(Duration.ofSeconds(5)).
                        pingInterval(Duration.ofSeconds(10)).
                        reconnectWait(Duration.ofSeconds(1)).
                        maxReconnects(-1).
                        traceConnection();


        builder = builder.connectionListener((conn, type) -> System.out.println("Status change "+type));

        builder = builder.errorListener(new ErrorListener() {
            @Override
            public void slowConsumerDetected(Connection conn, Consumer consumer) {
                System.out.println("NATS connection slow consumer detected");
            }

            @Override
            public void exceptionOccurred(Connection conn, Exception exp) {
                System.out.println("NATS connection exception occurred");
                exp.printStackTrace();
            }

            @Override
            public void errorOccurred(Connection conn, String error) {
                System.out.println("NATS connection error occurred " + error);
            }
        });

        if (System.getenv("NATS_NKEY") != null && System.getenv("NATS_NKEY") != "") {
            AuthHandler handler = new ExampleAuthHandler(System.getenv("NATS_NKEY"));
            builder.authHandler(handler);
        } else if (System.getenv("NATS_CREDS") != null && System.getenv("NATS_CREDS") != "") {
            builder.authHandler(Nats.credentials(System.getenv("NATS_CREDS")));
        }

        return builder.build();
    }

    public static void main(String args[]) {
        String server;

        if (args.length == 1) {
            server = args[0];
        } else {
            usage();
            return;
        }

        try {
            System.out.println();
            System.out.printf("Timing connect time to %s.\n", server);
            System.out.println();

            Options options = createOptions(server);

            long start = System.nanoTime();
            Connection nc = Nats.connect(options);
            long end = System.nanoTime();
            double seconds = ((double)(end - start)) / 1_000_000_000.0;
            System.out.printf("Connect time to %s was %.3f seconds\n", server, seconds);

            nc.close();
            
        } catch (Exception exp) {
            exp.printStackTrace();
        }
    }

    static void usage() {
        System.err.println(usageString);
        System.exit(-1);
    }
}