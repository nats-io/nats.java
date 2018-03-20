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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class NatsServer implements Runnable, AutoCloseable {
    private static final String GNATSD = "gnatsd";
    private static final String TEST_RESOURCE_DIR = "src/test/resources";

    // Enable this for additional server debugging info.
    private boolean debug = false;

    private ProcessBuilder pb;
    private Process p;
    private ProcessStartInfo psInfo;

    class ProcessStartInfo {
        final List<String> arguments = new ArrayList<String>();

        public ProcessStartInfo(String command) {
            this.arguments.add(command);
        }

        public void addArgument(String arg) {
            this.arguments.addAll(Arrays.asList(arg.split("\\s+")));
        }

        String[] getArgsAsArray() {
            return arguments.toArray(new String[arguments.size()]);
        }

        String getArgsAsString() {
            String stringVal = "";
            for (String str : arguments) {
                stringVal = stringVal.concat(str + " ");
            }
            return stringVal.trim();
        }

        public String toString() {
            return getArgsAsString();
        }
    }

    public NatsServer(boolean debug) {
        this(-1, debug);
    }

    public NatsServer() {
        this(-1, false);
    }

    public NatsServer(int port) {
        this(port, false);
    }

    public NatsServer(int port, boolean debug) {
        this.debug = debug;
        psInfo = this.createProcessStartInfo();

        if (port > 1023) {
            psInfo.addArgument("-p " + String.valueOf(port));
        }
        // psInfo.addArgument("-m 8222");

        start();
    }

    private String buildConfigFileName(String configFile) {
        return Paths.get(TEST_RESOURCE_DIR, configFile).toAbsolutePath().toString();
    }

    public NatsServer(String configFile, boolean debug) {
        this.debug = debug;
        psInfo = this.createProcessStartInfo();
        psInfo.addArgument("-config " + buildConfigFileName(configFile));
        start();
    }

    private ProcessStartInfo createProcessStartInfo() {

        // The NATS server must be on the path.
        psInfo = new ProcessStartInfo(GNATSD);

        if (debug) {
            psInfo.addArgument("-DV");
        }

        return psInfo;
    }

    private void start() {
        try {
            pb = new ProcessBuilder(psInfo.arguments);
            pb.directory(new File("target"));
            if (debug) {
                System.err.println("Inheriting IO, psInfo =" + psInfo);
                pb.inheritIO();
            } else {
                // All NATS server output goes to stderr
                String errFile = "/dev/null";

                String osName = System.getProperty("os.name");
                if (osName != null && osName.contains("Windows")) {
                    // Windows uses the "nul" file.
                    errFile = "nul";
                }

                pb.redirectError(new File(errFile));
            }
            p = pb.start();
            if (debug) {
                System.out.println("Started [" + psInfo + "]");
            }
        } catch (IOException e) {
            System.out.println("Unable to start [" + psInfo + "]. The NATS server " +
                    "must be installed and found in the path.\n" +
                    "See https://github.com/nats-io/gnatsd");
            System.out.println(e.getMessage());
        }
    }

    public void shutdown() {
        if (p == null) {
            return;
        }
        p.destroy();
        if (debug) {
            System.out.println("Stopped [" + psInfo + "]");
        }

        p = null;
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub

    }


    @Override
    public void close() {
        this.shutdown();
    }
}
