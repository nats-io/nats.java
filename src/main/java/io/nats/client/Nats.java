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

import java.io.IOException;

import io.nats.client.impl.NatsImpl;

public class Nats {

    /**
     * Current version of the library - {@value #CLIENT_VERSION}
     */
    public static final String CLIENT_VERSION = "2.0.0";

    /**
     * Current language of the library - {@value #CLIENT_LANGUAGE}
     */
    public static final String CLIENT_LANGUAGE = "java";

    /**
     * Connect to the default URL ({#value Options#DEFAULT_URL}) with all of the
     * default options.
     * 
     * <p>This is a synchronous call, and the connection should be ready for use on return
     * there are network timing issues that could result in a successful connect call but
     * the connection is invalid soon after return, where soon is in the network/thread world.
     * 
     * @throws IOException if a networking issue occurs
     * @throws InterruptedException if the current thread is interrupted
     * @return the connection
     */
    public static Connection connect() throws IOException, InterruptedException {
        Options options = new Options.Builder().server(Options.DEFAULT_URL).build();
        return createConnection(options, false);
    }

    /**
     * The Java client generally expects URLs of the form {@code nats://hostname:port}
     * 
     * <p>but also allows urls with a user password {@code nats://user:pass@hostname:port}.
     * 
     * <p>or token in them {@code nats://token@hostname:port}.
     * 
     * <p>Moreover, you can initiate a TLS connection, by using the `tls`
     * schema, whic will use the default SSLContext, or fail if one is not set. For
     * testing and development, the `opentls` schema is support when the server is
     * in non-verify mode. In this case, the client will accept any server
     * certificate and will not provide one of its own.
     * 
     * <p>This is a synchronous call, and the connection should be ready for use on return
     * there are network timing issues that could result in a successful connect call but
     * the connection is invalid soon after return, where soon is in the network/thread world.
     * 
     * @param url the url of the server, ie. nats://localhost:4222
     * @throws IOException if a networking issue occurs
     * @throws InterruptedException if the current thread is interrupted
     * @return the connection
     */
    public static Connection connect(String url) throws IOException, InterruptedException {
        Options options = new Options.Builder().server(url).build();
        return createConnection(options, false);
    }

    /**
     * Options can be used to set the server URL, or multiple URLS, callback
     * handlers for various errors, and connection events.
     * 
     * 
     * <p>This is a synchronous call, and the connection should be ready for use on return
     * there are network timing issues that could result in a successful connect call but
     * the connection is invalid soon after return, where soon is in the network/thread world.
     * 
     * @param options the options object to use to create the connection
     * @throws IOException if a networking issue occurs
     * @throws InterruptedException if the current thread is interrupted
     * @return the connection
     */
    public static Connection connect(Options options) throws IOException, InterruptedException {
        return createConnection(options, false);
    }

    /**
     * Try to connect in another thread, a connection listener is required to get
     * the connection.
     * 
     * <p>Normally connect will loop through the available servers one time. If
     * reconnectOnConnect is true, the connection attempt will repeat based on the
     * settings in options, including indefinitely.
     * 
     * <p>If there is an exception before a connection is created, and the error
     * listener is set, it will be notified with a null connection.
     * 
     * <p><strong>This method is experimental, please provide feedback on its value.</strong>
     * 
     * @param options            the connection options
     * @param reconnectOnConnect if true, the connection will treat the initial
     *                           connection as any other and attempt reconnects on
     *                           failure
     * 
     * @throws IllegalArgumentException if no connection listener is set in the options
     * @throws InterruptedException if the current thread is interrupted
     */
    public static void connectAsychronously(Options options, boolean reconnectOnConnect)
            throws InterruptedException {

        if (options.getConnectionListener() == null) {
            throw new IllegalArgumentException("Connection Listener required in connectAsychronously");
        }

        Thread t = new Thread(() -> {
            try {
                NatsImpl.createConnection(options, reconnectOnConnect);
            } catch (Exception ex) {
                if (options.getErrorListener() != null) {
                    options.getErrorListener().exceptionOccurred(null, ex);
                }
            }
        });
        t.start();
    }

    private static Connection createConnection(Options options, boolean reconnectOnConnect)
            throws IOException, InterruptedException {
        return NatsImpl.createConnection(options, reconnectOnConnect);
    }

    private Nats() {
        throw new UnsupportedOperationException("Nats is a static class");
    }
}