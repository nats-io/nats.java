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

package io.nats.client;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

// TODO(sasbury): support for config files

/**
 * Class to run gnatds for tests. Highly based on the 1.0 client's NatsServer code.
 */
public class NatsTestServer implements AutoCloseable {

    private static final String GNATSD = "gnatsd";

    // Use a new port each time, we increment and get so start at the normal port
    private static AtomicInteger portCounter = new AtomicInteger(Options.DEFAULT_PORT);

    private int port;
    private boolean debug;
    private Process process;
    private String cmdLine;

    public NatsTestServer() {
        this(false);
    }

    public NatsTestServer(boolean debug) {
        this(NatsTestServer.nextPort(), debug);
    }

    public NatsTestServer(int port, boolean debug) {
        this.port = port;
        this.debug = debug;
        start();
    }

    public static int nextPort() {
        return NatsTestServer.portCounter.incrementAndGet();
    }

    public void start() {
        ArrayList<String> cmd = new ArrayList<String>();
        cmd.add(NatsTestServer.GNATSD);
        cmd.add("-p"); cmd.add(String.valueOf(port));
        
        if (debug) {
            cmd.add("-DV");
        }

        this.cmdLine = String.join(" ", cmd);

        try {
            ProcessBuilder pb = new ProcessBuilder(cmd);
            // TODO(sasbury): do we need to set this? pb.directory();

            if (debug) {
                System.out.println("%%% Starting [" + this.cmdLine + "] with redirected IO");
                pb.inheritIO();
            } else {
                String errFile = null;
                String osName = System.getProperty("os.name");

                if (osName != null && osName.contains("Windows")) {
                    // Windows uses the "nul" file.
                    errFile = "nul";
                } else {                
                    errFile = "/dev/null";
                }

                pb.redirectError(new File(errFile));
            }

            this.process = pb.start();

            if (debug) {
                System.out.println("%%% Started [" + this.cmdLine + "]");
            }
        } catch (IOException ex) {
            System.out.println("%%% Failed to start [" + this.cmdLine + "] with message:");
            System.out.println("\t" + ex.getMessage());
            System.out.println("%%% Make sure that gnatsd is installed and in your PATH.");
            System.out.println("%%% See https://github.com/nats-io/gnatsd for information on installing gnatsd");
        }
    }

    public int getPort() {
        return this.port;
    }

    public void shutdown() {
        if (this.process == null) {
            return;
        }

        this.process.destroy();
        
        if (this.debug) {
            System.out.println("%%% Shut down ["+ this.cmdLine +"]");
        }

        this.process = null;
    }

    /**
     * Synonomous with shutdown.
     */
    public void close() {
        shutdown();
    }
}