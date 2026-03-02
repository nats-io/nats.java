// Copyright 2026 The NATS Authors
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

package io.nats.client.support;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

/**
 * Implements the "Happy Eyeballs" algorithm as described in RFC 6555/8305,
 * which attempts to connect to multiple IP addresses in parallel to reduce
 * connection setup delays.
 */
public class HappyEyeballsConnector {

    // Connection attempt delay per RFC 8305 section 5
    private static final int CONNECT_DELAY_MILLIS = 250;

    /**
     * Connect to the fastest responding IP for the given hostname.
     * @param executor the executor to run connection tasks on
     * @param socketCreator creates a new socket for each attempt
     * @param hostname the hostname to resolve and connect to
     * @param port the port to connect to
     * @param timeoutMillis connection timeout per attempt
     * @return a connected socket
     * @throws IOException if no IP could be reached
     */
    public static Socket connect(ExecutorService executor, Callable<Socket> socketCreator,
                                 String hostname, int port, int timeoutMillis) throws IOException
    {
        InetAddress[] ips = NatsInetAddress.getAllByName(hostname);

        // Short circuit for single IP
        if (ips.length == 1) {
            try {
                Socket socket = socketCreator.call();
                socket.connect(new InetSocketAddress(ips[0], port), timeoutMillis);
                return socket;
            }
            catch (IOException e) {
                throw e;
            }
            catch (Exception e) {
                throw new IOException(e);
            }
        }

        // Track all sockets so losers can be closed
        List<Socket> allSockets = Collections.synchronizedList(new ArrayList<>());

        // Create connection tasks with staggered delays (0ms, 250ms, 500ms, ...)
        List<Callable<Socket>> tasks = new ArrayList<>();
        for (int i = 0; i < ips.length; i++) {
            final InetAddress ip = ips[i];
            final int delayMillis = i * CONNECT_DELAY_MILLIS;

            tasks.add(() -> {
                if (delayMillis > 0) {
                    try {
                        Thread.sleep(delayMillis);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                }

                Socket socket = socketCreator.call();
                allSockets.add(socket);
                socket.connect(new InetSocketAddress(ip, port), timeoutMillis);
                return socket;
            });
        }

        try {
            Socket winner = executor.invokeAny(tasks);
            closeAllExcept(allSockets, winner);
            return winner;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (ExecutionException ignored) {}

        closeAllExcept(allSockets, null);
        throw new IOException("No responsive IP found for " + hostname);
    }

    private static void closeAllExcept(List<Socket> sockets, Socket keep) {
        for (Socket s : sockets) {
            if (s != keep) {
                try { s.close(); } catch (IOException ignore) {}
            }
        }
    }
}
