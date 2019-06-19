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

/**
 * The Nats class is the entry point into the NATS client for Java. This class
 * is used to create a connection to the NATS server. Connecting is a
 * synchronous process, with a new asynchronous version available for experimenting.
 * 
 * <p>Simple connections can be created with a URL, while more control is provided
 * when an {@link Options Options} object is used. There are a number of options that
 * effect every connection, as described in the {@link Options Options} documentation.
 * 
 * <p>At its simplest, you can connect to a nats-server on the local host using the default port with:
 * <pre>Connection nc = Nats.connect()</pre>
 * <p>and start sending or receiving messages immediately after that.
 * 
 * <p>While the simple case relies on a single URL, the options allows you to configure a list of servers
 * that is used at connect time and during reconnect scenarios.
 * 
 * <p>NATS supports TLS connections. This library relies on the standard SSLContext class to configure
 * SSL certificates and trust managers, as a result there are two steps to setting up a TLS connection,
 * configuring the SSL context and telling the library which one to use. Several options are provided
 * for each. To tell the library to connect with TLS:
 * 
 * <ul>
 * <li>Pass a tls:// URL to the connect method, or as part of the options. The library will use the 
 * default SSLContext for both the client certificates and trust managers.
 * <li>Call the {@link Options.Builder#secure() secure} method on the options builder, again the default
 * SSL Context is used.
 * <li>Call {@link Options.Builder#sslContext(javax.net.ssl.SSLContext) sslContext} when building your options.
 * Your context will be used.
 * <li>Pass an opentls:// url to the connect method, or in the options. The library will create a special
 * SSLContext that has no client certificates and trusts any server. <strong>This is less secure, but useful for
 * testing and behind a firewall.</strong>
 * <li>Call the {@link Options.Builder#opentls() opentls} method on the builder when creating your options, again
 * the all trusting, non-verifiable client is created.
 * </ul>
 * 
 * <p>To set up the default context for tls:// or {@link Options.Builder#secure() secure} you can:
 * <ul>
 * <li>Configure the default using System properties, i.e. <em>javax.net.ssl.keyStore</em>.
 * <li>Set the context manually with the SSLContext setDefault method.
 * </ul>
 * 
 * <p>If the server is configured to verify clients, the opentls mode will not work, and the other modes require a client certificate
 * to work.
 * 
 * <p>Authentication, if configured on the server, is managed via the Options as well. However, the url passed to {@link #connect(String) connect()}
 * can provide a user/password pair or a token using the forms: {@code nats://user:password@server:port} and {@code nats://token@server:port}.
 * 
 * <p>Regardless of the method used a {@link Connection Connection} object is created, and provides the methods for
 * sending, receiving and dispatching messages.
 */
public class Nats {

    /**
     * Current version of the library - {@value #CLIENT_VERSION}
     */
    public static final String CLIENT_VERSION = "2.5.2";

    /**
     * Current language of the library - {@value #CLIENT_LANGUAGE}
     */
    public static final String CLIENT_LANGUAGE = "java";

    /**
     * Connect to the default URL, {@link Options#DEFAULT_URL Options.DEFAULT_URL}, with all of the
     * default options.
     * 
     * <p>This is a synchronous call, and the connection should be ready for use on return
     * there are network timing issues that could result in a successful connect call but
     * the connection is invalid soon after return, where soon is in the network/thread world.
     * 
     * <p>If the connection fails, an IOException is thrown
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
     * schema, which will use the default SSLContext, or fail if one is not set. For
     * testing and development, the `opentls` schema is support when the server is
     * in non-verify mode. In this case, the client will accept any server
     * certificate and will not provide one of its own.
     * 
     * <p>This is a synchronous call, and the connection should be ready for use on return
     * there are network timing issues that could result in a successful connect call but
     * the connection is invalid soon after return, where soon is in the network/thread world.
     * 
     * <p>If the connection fails, an IOException is thrown
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
     * <p>If the connection fails, an IOException is thrown
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
    public static void connectAsynchronously(Options options, boolean reconnectOnConnect)
            throws InterruptedException {

        if (options.getConnectionListener() == null) {
            throw new IllegalArgumentException("Connection Listener required in connectAsynchronously");
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
        t.setName("NATS - async connection");
        t.start();
    }

    /**
     * Create an authhandler from a chain file. The handler will read the file each time it needs to respond to a request
     * and clear the memory after. This has a small price, but will only be encountered during connect or reconnect.
     * 
     * The chain file has a JWT - generally commented with a separator - followed by an nkey - also with a separator.
     * 
     * @param chainFile a file containing a user JWT and an nkey
     * @return an authhandler that will use the chain file to load/clear the nkey and jwt as needed
     */
    public static AuthHandler credentials(String chainFile) {
        return NatsImpl.credentials(chainFile);
    }

    /**
     * Create an authhandler from a jwt file and an nkey file. The handler will read the files each time it needs to respond to a request
     * and clear the memory after. This has a small price, but will only be encountered during connect or reconnect.
     * 
     * @param jwtFile a file containing a user JWT, may or may not contain separators
     * @param nkeyFile a file containing a user nkey that matches the JWT, may or may not contain separators
     * @return an authhandler that will use the chain file to load/clear the nkey and jwt as needed
     */
    public static AuthHandler credentials(String jwtFile, String nkeyFile) {
        return NatsImpl.credentials(jwtFile, nkeyFile);
    }

    private static Connection createConnection(Options options, boolean reconnectOnConnect)
            throws IOException, InterruptedException {
        return NatsImpl.createConnection(options, reconnectOnConnect);
    }

    private Nats() {
        throw new UnsupportedOperationException("Nats is a static class");
    }
}
