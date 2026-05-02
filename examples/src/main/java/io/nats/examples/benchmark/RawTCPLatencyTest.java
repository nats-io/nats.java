package io.nats.examples.benchmark;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RawTCPLatencyTest {

    static boolean isServer;
    static String host = "localhost";
    static int port = 1234;
    static int warmIters = 1000, runIters = 10000;

    static InputStream in;
    static OutputStream out;

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Parameters: server/client host port");
            return;
        }
        isServer = args[0].startsWith("s");
        if (args.length > 1) {
            host = args[1];
        }
        if (args.length > 2) {
            port = Integer.parseInt(args[2]);
        }
        try {
            if (isServer) {
                runServer();
            } else {
                runClient();
            }
        } catch (IOException ex) {
            Logger.getLogger(RawTCPLatencyTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static void runServer() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            while (true) {
                Socket socket = serverSocket.accept();
                System.out.println("Connected");
                socket.setTcpNoDelay(true);
                socket.setReceiveBufferSize(2 * 1024 * 1024);
                socket.setSendBufferSize(2 * 1024 * 1024);
                in = socket.getInputStream();
                out = socket.getOutputStream();
                try {
                    while (true) {
                        int rq = in.read();
                        out.write(rq);
                    }
                } catch (IOException e) {
                    System.out.println("Disconnected");
                }
            }
        }
    }

    private static void runClient() throws SocketException, IOException {
        Socket socket = new Socket();
        socket.setTcpNoDelay(true);
        socket.setReceiveBufferSize(2 * 1024 * 1024);
        socket.setSendBufferSize(2 * 1024 * 1024);
        socket.connect(new InetSocketAddress(host, port), 1000);
        in = socket.getInputStream();
        out = socket.getOutputStream();
        System.out.println("Connected");
        for (int i = 0; i < warmIters; i++) {
            sendRecv();
        }
        System.out.println("Warmed");
        long t0 = System.nanoTime();
        for (int i = 0; i < runIters; i++) {
            sendRecv();
        }
        long t1 = System.nanoTime();
        System.out.println("Average latency " + (1.0 * (t1 - t0)) / (1000000.0 * runIters) + " ms");
        socket.close();
    }

    private static void sendRecv() throws IOException {
        out.write(11);
        in.read();
    }
}
