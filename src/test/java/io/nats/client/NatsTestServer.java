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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class to run gnatds for tests. Highly based on the 1.0 client's NatsServer code.
 */
public class NatsTestServer implements AutoCloseable {

    private static final String NATS_SERVER = "gnatsd";

    // Use a new port each time, we increment and get so start at the normal port
    private static AtomicInteger portCounter = new AtomicInteger(Options.DEFAULT_PORT + 1);

    private int port;
    private boolean debug;
    private String configFilePath;
    private Process process;
    private String cmdLine;
    private String[] customArgs;
    private String[] configInserts;

    public static String generateNatsServerVersionString() {
        ArrayList<String> cmd = new ArrayList<String>();

        String server_path = System.getenv("nats_server_path");

        if(server_path == null){
            server_path = NatsTestServer.NATS_SERVER;
        }

        cmd.add(server_path);
        cmd.add("--version");

        try {
            ProcessBuilder pb = new ProcessBuilder(cmd);
            Process process = pb.start();
            process.waitFor();
			BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            ArrayList<String> lines = new ArrayList<String>();
            String line = "";			
			while ((line = reader.readLine())!= null) {
				lines.add(line);
            }
            
            if (lines.size() > 0) {
                return lines.get(0);
            }

            return null;
        }
        catch (Exception exp) {
            return null;
        }
    }

    public static int nextPort() {
        return NatsTestServer.portCounter.incrementAndGet();
    }

    public static int currentPort() {
        return NatsTestServer.portCounter.get();
    }

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

    public NatsTestServer(String configFilePath, boolean debug) {
        this.configFilePath = configFilePath;
        this.debug = debug;
        this.port = nextPort();
        start();
    }

    public NatsTestServer(String configFilePath, String[] inserts, int port, boolean debug) {
        this.configFilePath = configFilePath;
        this.configInserts = inserts;
        this.debug = debug;
        this.port = port;
        start();
    }

    public NatsTestServer(String configFilePath, int port, boolean debug) {
        this.configFilePath = configFilePath;
        this.debug = debug;
        this.port = port;
        start();
    }

    public NatsTestServer(String[] customArgs, boolean debug) {
        this.port = NatsTestServer.nextPort();
        this.debug = debug;
        this.customArgs = customArgs;
        start();
    }

    public NatsTestServer(String[] customArgs, int port, boolean debug) {
        this.port = port;
        this.debug = debug;
        this.customArgs = customArgs;
        start();
    }

    public void start() {
        ArrayList<String> cmd = new ArrayList<String>();

        String server_path = System.getenv("nats_server_path");

        if(server_path == null){
            server_path = NatsTestServer.NATS_SERVER;
        }

        cmd.add(server_path);

        // Rewrite the port to a new one, so we don't reuse the same one over and over
        if (this.configFilePath != null) {
            Pattern pattern = Pattern.compile("port: (\\d+)");
            Matcher matcher = pattern.matcher("");
            BufferedReader read = null;
            File tmp = null;
            BufferedWriter write = null;
            String line;

            try {
                tmp = File.createTempFile("nats_java_test", ".conf");
                write = new BufferedWriter(new FileWriter(tmp));
                read = new BufferedReader(new FileReader(this.configFilePath));

                while ((line = read.readLine()) != null) {
                    matcher.reset(line);

                    if (matcher.find()) {
                        line = line.replace(matcher.group(1), String.valueOf(this.port));
                    }

                    write.write(line);
                    write.write("\n");
                }

                if (configInserts != null) {
                    for (String s : configInserts) {
                        write.write(s);
                        write.write("\n");
                    }
                }
            } catch (Exception exp) {
                System.out.println("%%% Error parsing config file for port.");
                return;
            } finally {
                if (read != null) {
                    try {
                        read.close();
                    } catch (Exception e) {
                        throw new IllegalStateException("Failed to read config file");
                    }
                }
                if (write != null) {
                    try{
                        write.close();
                    } catch (Exception e) {
                        throw new IllegalStateException("Failed to update config file");
                    }
                }
            }

            cmd.add("--config");
            cmd.add(tmp.getAbsolutePath());
        } else {
            cmd.add("--port");
            cmd.add(String.valueOf(port));
        }

        if (this.customArgs != null) {
            cmd.addAll(Arrays.asList(this.customArgs));
        }
            
        if (debug) {
            cmd.add("-DV");
        }

        this.cmdLine = String.join(" ", cmd);

        try {
            ProcessBuilder pb = new ProcessBuilder(cmd);

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
            
            int tries = 10;
            // wait at least 1x and maybe 10
            do {
                try {
                    Thread.sleep(100);
                } catch (Exception exp) {
                    //Give the server time to get going
                }
                tries--;
            } while(!this.process.isAlive() && tries > 0);

            System.out.println("%%% Started [" + this.cmdLine + "]");
        } catch (IOException ex) {
            System.out.println("%%% Failed to start [" + this.cmdLine + "] with message:");
            System.out.println("\t" + ex.getMessage());
            System.out.println("%%% Make sure that the nats-server is installed and in your PATH.");
            System.out.println("%%% See https://github.com/nats-io/nats-server for information on installation");

            throw new IllegalStateException("Failed to run [" + this.cmdLine +"]");
        }
    }

    public int getPort() {
        return this.port;
    }

    public String getURI() {
        return getURIForPort(this.port);
    }

    public static String getURIForPort(int port) {
        return "nats://localhost:" + port;
    }

    public void shutdown() {

        if (this.process == null) {
            return;
        }

        this.process.destroy();
        
        System.out.println("%%% Shut down ["+ this.cmdLine +"]");

        this.process = null;
    }

    /**
     * Synonomous with shutdown.
     */
    public void close() {
        shutdown();
    }
}