/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class NATSServer implements Runnable, AutoCloseable {
    final static String GNATSD = "gnatsd";
    final static String TEST_RESOURCE_DIR = "src/test/resources";

    // Enable this for additional server debugging info.
    boolean debug = false;

    ProcessBuilder pb;
    Process p;
    ProcessStartInfo psInfo;

    class ProcessStartInfo {
        List<String> arguments = new ArrayList<String>();

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
            String stringVal = new String();
            for (String s : arguments)
                stringVal = stringVal.concat(s + " ");
            return stringVal.trim();
        }

        public String toString() {
            return getArgsAsString();
        }
    }

    public NATSServer(boolean debug) {
        this(-1, debug);
    }

    public NATSServer() {
        this(-1, false);
    }

    public NATSServer(int port) {
        this(port, false);
    }

    public NATSServer(int port, boolean debug) {
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

    public NATSServer(String configFile, boolean debug) {
        this.debug = debug;
        psInfo = this.createProcessStartInfo();
        psInfo.addArgument("-config " + buildConfigFileName(configFile));
        start();
    }

    private ProcessStartInfo createProcessStartInfo() {
        String path = Paths.get("target", "/", GNATSD).toAbsolutePath().toString();
        psInfo = new ProcessStartInfo(path);

        if (debug) {
            psInfo.addArgument("-DV");
        }

        return psInfo;
    }

    public void start() {
        try {
            pb = new ProcessBuilder(psInfo.arguments);
            pb.directory(new File("target"));
            if (debug) {
                System.err.println("Inheriting IO, psInfo =" + psInfo);
                pb.inheritIO();
            } else {
                pb.redirectError(new File("/dev/null"));
                pb.redirectOutput(new File("/dev/null"));
            }
            p = pb.start();
            if (debug) {
                System.out.println("Started [" + psInfo + "]");
            }
        } catch (IOException e) {
            e.printStackTrace();
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
