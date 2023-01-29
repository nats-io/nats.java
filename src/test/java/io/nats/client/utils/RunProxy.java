// Copyright 2021 The NATS Authors
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

package io.nats.client.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.SSLContext;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Quick proxy server implementation for testing against.
 */
public class RunProxy implements Runnable {
    private static final Pattern REQUEST_LINE_PATTERN = Pattern.compile("^(\\S+)\\s+([^\\s:]+):(\\d+)\\s+HTTP/(\\S+)$");

    private ServerSocket server;
    private InetSocketAddress hostPort;
    private ExecutorService executor;

    /**
     * Specify port=0 for ephemeral port. Specify null SSLContext to
     * NOT make it a secure proxy.
     * @throws IOException if we fail to bind to the host:port specified.
     */
    public RunProxy(InetSocketAddress hostPort, SSLContext sslContext, ExecutorService executor) throws IOException {
        if (null == hostPort.getHostName()) {
            throw new IllegalArgumentException("Expected URI with host defined, but got " + hostPort);
        }
        int port = hostPort.getPort();
        if (port < 0) {
            port = 0;
        }
        if (null != sslContext) {
            server = sslContext.getServerSocketFactory().createServerSocket(
                port,
                10,
                hostPort.getAddress());
        } else {
            server = new ServerSocket(
                port,
                10,
                hostPort.getAddress());
        }
        this.hostPort = new InetSocketAddress(hostPort.getHostName(), server.getLocalPort());
        this.executor = executor;
    }

    /**
     * Runs the "accept" thread which delegates sockets out to the executor.
     */
	@Override
	public void run() {
        try {
            runImpl();
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
	}

    public int getPort() {
        return hostPort.getPort();
    }

    private void runImpl() throws IOException {
        while (true) {
            Socket client = server.accept();
            executor.submit(() -> proxy(client));
        }
    }

    private void proxy(Socket client) {
        Socket remote;
        try {
            remote = handshake(client);
            if (null == remote) {
                return;
            }
        } catch (Exception ex) {
            System.err.println("Failure to establish a proxy:");
            ex.printStackTrace(System.err);
            try {
                client.close();
            } catch (Exception ignored) {}
            return;
        }
        executor.submit(() -> transferTo(remote, client, "remote -> client"));
        executor.submit(() -> transferTo(client, remote, "client -> remote"));
    }

    private void transferTo(Socket from, Socket to, String description) {
        try {
            InputStream in = from.getInputStream();
            OutputStream out = to.getOutputStream();
            // Really inefficient!
            while (true) {
                int ch = in.read();
                if (ch < 0) {
                    return;
                }
                out.write(ch);
            }
        } catch (Exception ex) {
            System.err.println("Failure to forward data (" + description + "):");
            ex.printStackTrace(System.err);
        }
    }

    private Socket handshake(Socket client) throws IOException {
        String line = getLatin1Line(client.getInputStream());
        Matcher matcher = REQUEST_LINE_PATTERN.matcher(line);
        if (!matcher.matches()) {
            System.err.println("proxy received unexpected request line=" + line);
            return null;
        }
        if (!"CONNECT".equalsIgnoreCase(matcher.group(1))) {
            System.err.println("Unsupported method='" + matcher.group(1) + "'");
            client.getOutputStream().write(
                "HTTP/1.0 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n".getBytes(UTF_8));
            return null;
        }
        while (true) {
            line = getLatin1Line(client.getInputStream());
            if (null == line) {
                System.err.println("Premature close...");
                return null;
            }
            if ("".equals(line)) {
                break;
            }
        }
        String remoteHost = matcher.group(2);
        int remotePort = Integer.parseInt(matcher.group(3));
        Socket remote;
        try {
            remote = new Socket();
            remote.setTcpNoDelay(true);
            remote.setReceiveBufferSize(2 * 1024 * 1024);
            remote.setSendBufferSize(2 * 1024 * 1024);
            remote.connect(new InetSocketAddress(remoteHost, remotePort));
            remote = new Socket(remoteHost, remotePort);
        } catch (Exception ex) {
            System.err.println("Remote connect to " + remoteHost + ":" + remotePort + " failed:");
            ex.printStackTrace(System.err);
            client.getOutputStream().write(
                "HTTP/1.0 400 Bad Connection\r\nContent-Length: 0\r\n\r\n".getBytes(UTF_8));
            return null;
        }
        client.getOutputStream().write(
            "HTTP/1.0 200 OK\r\n\r\n".getBytes(UTF_8));
        return remote;
    }

    /**
     * Only handles \r\n, not robust!
     */
    private String getLatin1Line(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        boolean gotCR = false;
        while (true) {
            int ch = in.read();
            if (ch < 1) {
                // Got end of stream.
                return sb.length() > 0 ? sb.toString() : null;
            }
            switch (ch) {
            case '\r':
                gotCR = true;
                break;

            case '\n':
                if (gotCR) {
                    return sb.deleteCharAt(sb.length() - 1).toString();
                }
            }
            sb.append((char)(ch & 0xFF));
        }
    }
}
