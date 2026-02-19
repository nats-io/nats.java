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

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;

import static io.nats.client.support.Encoding.base64UrlEncodeToString;

/**
 * Handles the begining of the connect sequence, all hard coded, but
 * is configurable to fail at specific points to allow client testing.
 */
public class NatsServerProtocolMock implements Closeable, TestServer {

    // Default is to exit after pong
    public enum ExitAt {
        SLEEP_BEFORE_INFO,
        EXIT_BEFORE_INFO,
        EXIT_AFTER_INFO,
        EXIT_AFTER_CONNECT,
        EXIT_AFTER_PING,
        EXIT_AFTER_CUSTOM,
        NO_EXIT
    }

    public enum Progress {
        NO_CLIENT,
        CLIENT_CONNECTED,
        SENT_INFO,
        GOT_CONNECT,
        GOT_PING,
        SENT_PONG,
        STARTED_CUSTOM_CODE,
        COMPLETED_CUSTOM_CODE,
    }

    public interface Customizer {
        void customizeTest(NatsServerProtocolMock mockTs, BufferedReader reader, PrintWriter writer);
    }

    private final int port;
    private final ExitAt exitAt;
    private Progress progress;
    private boolean protocolFailure;
    private CompletableFuture<Boolean> waitForIt;
    private Customizer customizer;
    private String customInfo;
    private String separator = " ";

    private boolean customInfoIsFullInfo = false;

    public NatsServerProtocolMock(ExitAt exitAt) throws IOException {
        this(NatsTestServer.nextPort(), exitAt);
    }

    public NatsServerProtocolMock(int port, ExitAt exitAt) {
        this.port = port;
        this.exitAt = exitAt;
        start();
    }

    public NatsServerProtocolMock(Customizer custom) throws IOException {
        this.port = NatsTestServer.nextPort();
        this.exitAt = ExitAt.NO_EXIT;
        this.customizer = custom;
        start();
    }

    public NatsServerProtocolMock(Customizer custom, int port, boolean exitAfterCustom) {
        this.port = port;

        if (exitAfterCustom) {
            this.exitAt = ExitAt.EXIT_AFTER_CUSTOM;
        } else {
            this.exitAt = ExitAt.NO_EXIT;
        }
        this.customizer = custom;
        start();
    }
    
    // CustomInfo is just the JSON string, not the full protocol string 
    // or the \r\n.
    public NatsServerProtocolMock(Customizer custom, String customInfo) throws IOException {
        this.port = NatsTestServer.nextPort();
        this.exitAt = ExitAt.NO_EXIT;
        this.customizer = custom;
        this.customInfo = customInfo;
        start();
    }

    private void start() {
        this.progress = Progress.NO_CLIENT;
        this.waitForIt = new CompletableFuture<>();
        Thread t = new Thread(this::accept);
        t.start();
        try {
            Thread.sleep(100);
        } catch (Exception exp) {
            //Give the server time to get going
        }
    }

    public void useTabs() {
        this.separator = "\t";
    }

    public void useCustomInfoAsFullInfo() {
        customInfoIsFullInfo = true;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public String getServerUri() {
        return "nats://0.0.0.0:" + port;
    }

    public Progress getProgress() {
        return this.progress;
    }

    // True if the failure was not intentional
    public boolean wasProtocolFailure() {
        return protocolFailure;
    }

    public void close() {
        waitForIt.complete(Boolean.TRUE);
    }

    public void accept() {
        ServerSocket serverSocket = null;
        Socket socket = null;
        PrintWriter writer = null;
        BufferedReader reader = null;

        try {
            serverSocket = new ServerSocket(this.port);
            serverSocket.setSoTimeout(5000);

            // System.out.println("*** Mock Server @" + this.port + " started...");
            socket = serverSocket.accept();
            
            this.progress = Progress.CLIENT_CONNECTED;
            // System.out.println("*** Mock Server @" + this.port + " got client...");

            writer = new PrintWriter(socket.getOutputStream());
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            if (exitAt == ExitAt.EXIT_BEFORE_INFO) {
                throw new Exception("exit");
            }

            if (exitAt == ExitAt.SLEEP_BEFORE_INFO) {
                try {
                    Thread.sleep(3000);
                } catch ( InterruptedException e) {
                    // ignore
                }
            }
            
            String encodedNonce = base64UrlEncodeToString("abcdefg".getBytes());

            if (this.customInfo != null) {
                if (customInfoIsFullInfo) {
                    writer.write(customInfo);
                } else {
                    writer.write("INFO" + this.separator + customInfo + "\r\n");
                }
            } else {
                writer.write("INFO" + this.separator + "{\"server_id\":\"test\", \"version\":\"9.9.99\", \"nonce\":\""+encodedNonce+"\", \"headers\":true}\r\n");
            }
            writer.flush();
            this.progress = Progress.SENT_INFO;
            // System.out.println("*** Mock Server @" + this.port + " sent info...");

            if (exitAt == ExitAt.EXIT_AFTER_INFO) {
                throw new Exception("exit");
            }

            String connect = reader.readLine();

            if (connect != null && connect.startsWith("CONNECT")) {
                this.progress = Progress.GOT_CONNECT;
                // System.out.println("*** Mock Server @" + this.port + " got connect...");
            } else {
                throw new IOException("First message wasn't CONNECT");
            }

            if (exitAt == ExitAt.EXIT_AFTER_CONNECT) {
                throw new Exception("exit");
            }

            String ping = reader.readLine();

            if (ping.startsWith("PING")) {
                this.progress = Progress.GOT_PING;
                // System.out.println("*** Mock Server @" + this.port + " got ping...");
            } else {
                throw new IOException("Second message wasn't PING");
            }

            if (exitAt == ExitAt.EXIT_AFTER_PING) {
                throw new Exception("exit");
            }

            writer.write("PONG\r\n");
            writer.flush();
            this.progress = Progress.SENT_PONG;
            // System.out.println("*** Mock Server @" + this.port + " sent pong...");

            if (this.customizer != null) {
                this.progress = Progress.STARTED_CUSTOM_CODE;
                // System.out.println("*** Mock Server @" + this.port + " starting custom code...");
                this.customizer.customizeTest(this, reader, writer);
                this.progress = Progress.COMPLETED_CUSTOM_CODE;
            }

            if (exitAt == ExitAt.EXIT_AFTER_CUSTOM) {
                throw new Exception("exit");
            }
            waitForIt.get(); // Wait for the test to cancel us

        } catch (IOException io) {
            protocolFailure = true;
            // System.out.println("\n*** Mock Server @" + this.port + " got exception "+io.getMessage());
            // io.printStackTrace();
        } catch (Exception ex) {
            // System.out.println("\n*** Mock Server @" + this.port + " got exception "+ex.getMessage());
            
            if (!"exit".equals(ex.getMessage())) {
                ex.printStackTrace();
            }
        }
        finally {
            if (serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (IOException ex) {
                    // System.out.println("\n*** Mock Server @" + this.port + " got exception "+ex.getMessage());
                }
            }
            if (socket != null) {
                try {
                    writer.close();
                    reader.close();
                    socket.close();
                } catch (IOException ex) {
                    // System.out.println("\n*** Mock Server @" + this.port + " got exception "+ex.getMessage());
                }
            }
        }
        // System.out.println("*** Mock Server @" + this.port + " completed...");
    }
}