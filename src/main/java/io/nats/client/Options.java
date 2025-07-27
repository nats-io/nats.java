// Copyright 2015-2025 The NATS Authors
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

import io.nats.client.impl.*;
import io.nats.client.support.*;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.CharBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static io.nats.client.support.Encoding.*;
import static io.nats.client.support.NatsConstants.*;
import static io.nats.client.support.SSLUtils.DEFAULT_TLS_ALGORITHM;
import static io.nats.client.support.Validator.*;

/**
 * The Options class specifies the connection options for a new NATs connection, including the default options.
 * Options are created using a {@link Options.Builder Builder}.
 * This class and the builder associated with it, is basically a long list of parameters. The documentation attempts
 * to clarify the value of each parameter in place on the builder and here, but it may be easier to read the documentation
 * starting with the {@link Options.Builder Builder}, since it has a simple list of methods that configure the connection.
 */
public class Options {
    // ----------------------------------------------------------------------------------------------------
    // NOTE TO DEVS!!! To add an option, you have to address:
    // ----------------------------------------------------------------------------------------------------
    // CONSTANTS * optionally add a default value constant
    // ENVIRONMENT PROPERTIES * always add an environment property. Constant always starts with PFX, but code accepts without
    // PROTOCOL CONNECT OPTION CONSTANTS * not related to options, but here because Options code uses them
    // CLASS VARIABLES * add a variable to the class
    // BUILDER VARIABLES * add a variable in builder
    // BUILD CONSTRUCTOR PROPS * update build props constructor to read new props
    // BUILDER METHODS * add a chainable method in builder for new variable
    // BUILD IMPL * update build() implementation if needed
    // BUILDER COPY CONSTRUCTOR * update builder constructor to ensure new variables are set
    // CONSTRUCTOR * update constructor to ensure new variables are set from builder
    // GETTERS * update getter to be able to retrieve class variable value
    // HELPER FUNCTIONS * just helpers
    // ----------------------------------------------------------------------------------------------------
    // README - if you add a property or change its comment, add it to or update the readme
    // ----------------------------------------------------------------------------------------------------

    // ----------------------------------------------------------------------------------------------------
    // CONSTANTS
    // ----------------------------------------------------------------------------------------------------
    /**
     * Default server URL. This property is defined as {@value}
     */
    public static final String DEFAULT_URL = "nats://localhost:4222";

    /**
     * Default server port. This property is defined as {@value}
     */
    public static final int DEFAULT_PORT = NatsConstants.DEFAULT_PORT;

    /**
     * Default maximum number of reconnect attempts, see {@link #getMaxReconnect() getMaxReconnect()}.
     * This property is defined as {@value}
     */
    public static final int DEFAULT_MAX_RECONNECT = 60;

    /**
     * Default wait time before attempting reconnection to the same server, see {@link #getReconnectWait() getReconnectWait()}.
     * This property is defined as 2000 milliseconds (2 seconds).
     */
    public static final Duration DEFAULT_RECONNECT_WAIT = Duration.ofMillis(2000);

    /**
     * Default wait time before attempting reconnection to the same server, see {@link #getReconnectJitter() getReconnectJitter()}.
     * This property is defined as 100 milliseconds.
     */
    public static final Duration DEFAULT_RECONNECT_JITTER = Duration.ofMillis(100);

    /**
     * Default wait time before attempting reconnection to the same server, see {@link #getReconnectJitterTls() getReconnectJitterTls()}.
     * This property is defined as 1000 milliseconds (1 second).
     */
    public static final Duration DEFAULT_RECONNECT_JITTER_TLS = Duration.ofMillis(1000);

    /**
     * Default connection timeout, see {@link #getConnectionTimeout() getConnectionTimeout()}.
     * This property is defined as 2 seconds.
     */
    public static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(2);

    /**
     * Default socket write timeout, see {@link #getSocketWriteTimeout() getSocketWriteTimeout()}.
     * This property is defined as 1 minute
     */
    public static final Duration DEFAULT_SOCKET_WRITE_TIMEOUT = Duration.ofMinutes(1);

    /**
     * Constant used for calculating if a socket write timeout is large enough.
     */
    public static final long MINIMUM_SOCKET_WRITE_TIMEOUT_GT_CONNECTION_TIMEOUT = 100;

    /**
     * Constant used for calculating if a socket read timeout is large enough.
     */
    public static final long MINIMUM_SOCKET_READ_TIMEOUT_GT_CONNECTION_TIMEOUT = 100;

    /**
     * Default server ping interval. The client will send a ping to the server on this interval to insure liveness.
     * The server may send pings to the client as well, these are handled automatically by the library,
     * see {@link #getPingInterval() getPingInterval()}.
     * <p>A value of {@code <=0} means disabled.</p>
     * <p>This property is defined as 2 minutes.</p>
     */
    public static final Duration DEFAULT_PING_INTERVAL = Duration.ofMinutes(2);

    /**
     * Default interval to clean up cancelled/timed out requests.
     * A timer is used to clean up futures that were handed out but never completed
     * via a message, {@link #getRequestCleanupInterval() getRequestCleanupInterval()}.
     * <p>This property is defined as 5 seconds.</p>
     */
    public static final Duration DEFAULT_REQUEST_CLEANUP_INTERVAL = Duration.ofSeconds(5);

    /**
     * Default maximum number of pings have not received a response allowed by the
     * client, {@link #getMaxPingsOut() getMaxPingsOut()}.
     * <p>This property is defined as {@value}</p>
     */
    public static final int DEFAULT_MAX_PINGS_OUT = 2;

    /**
     * Default SSL protocol used to create an SSLContext if the {@link #PROP_SECURE
     * secure property} is used.
     * <p>This property is defined as {@value}</p>
     */
    public static final String DEFAULT_SSL_PROTOCOL = "TLSv1.2";

    /**
     * Default of pending message buffer that is used for buffering messages that
     * are published during a disconnect/reconnect, {@link #getReconnectBufferSize() getReconnectBufferSize()}.
     * <p>This property is defined as {@value} bytes, 8 * 1024 * 1024.</p>
     */
    public static final int DEFAULT_RECONNECT_BUF_SIZE = 8_388_608;

    /**
     * The default length, {@value} bytes, the client will allow in an
     *  outgoing protocol control line, {@link #getMaxControlLine() getMaxControlLine()}.
     * <p>This value is configurable on the server, and should be set here to match.</p>
     */
    public static final int DEFAULT_MAX_CONTROL_LINE = 4096;

    /**
     * Default dataport class, which will use a TCP socket, {@link #getDataPortType() getDataPortType()}.
     * <p><em>This option is currently provided only for testing, and experimentation, the default
     * should be used in almost all cases.</em></p>
     */
    public static final String DEFAULT_DATA_PORT_TYPE = SocketDataPort.class.getCanonicalName();

    /**
     * Default size for buffers in the connection, not as available as other settings,
     * this is primarily changed for testing, {@link #getBufferSize() getBufferSize()}.
     */
    public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

    /**
     * Default thread name prefix. Used by the default executor when creating threads.
     * This property is defined as {@value}
     */
    public static final String DEFAULT_THREAD_NAME_PREFIX = "nats";

    /**
     * Default prefix used for inboxes, you can change this to manage authorization of subjects.
     * See {@link #getInboxPrefix() getInboxPrefix()}, the . is required but will be added if missing.
     */
    public static final String DEFAULT_INBOX_PREFIX = "_INBOX.";

    /**
     * This value is used internally to limit the number of messages sent in a single network I/O.
     * The value returned by {@link #getBufferSize() getBufferSize()} is used first, but if the buffer
     * size is large and the message sizes are small, this limit comes into play.
     * The choice of 1000 is arbitrary and based on testing across several operating systems. Use buffer
     * size for tuning.
     */
    public static final int MAX_MESSAGES_IN_NETWORK_BUFFER = 1000;

    /**
     * This value is used internally to limit the number of messages allowed in the outgoing queue. When
     * this limit is reached, publish requests will be blocked until the queue can clear.
     * Because this value is in messages, the memory size associated with this value depends on the actual
     * size of messages. If 0 byte messages are used, then DEFAULT_MAX_MESSAGES_IN_OUTGOING_QUEUE will take up the minimal
     * space. If 1024 byte messages are used then approximately 5Mb is used for the queue (plus overhead for subjects, etc..)
     * We are using messages, not bytes, to allow a simplification in the underlying library, and use LinkedBlockingQueue as
     * the core element in the queue.
     */
    public static final int DEFAULT_MAX_MESSAGES_IN_OUTGOING_QUEUE = 5000;

    /**
     * This value is used internally to discard messages when the outgoing queue is full.
     * See {@link #DEFAULT_MAX_MESSAGES_IN_OUTGOING_QUEUE}
     */
    public static final boolean DEFAULT_DISCARD_MESSAGES_WHEN_OUTGOING_QUEUE_FULL = false;

    /**
     * Default supplier for creating a single-threaded executor service.
     */
    public static final Supplier<ExecutorService> DEFAULT_SINGLE_THREAD_EXECUTOR = Executors::newSingleThreadExecutor;

    // ----------------------------------------------------------------------------------------------------
    // ENVIRONMENT PROPERTIES
    // ----------------------------------------------------------------------------------------------------
    static final String PFX = "io.nats.client.";
    static final int PFX_LEN = PFX.length();

    /**
     * Property used to configure a builder from a Properties object. {@value}, see
     * {@link Builder#connectionListener(ConnectionListener) connectionListener}.
     */
    public static final String PROP_CONNECTION_CB = PFX + "callback.connection";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see
     * {@link Builder#dataPortType(String) dataPortType}.
     */
    public static final String PROP_DATA_PORT_TYPE = PFX + "dataport.type";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see
     * {@link Builder#errorListener(ErrorListener) errorListener}.
     */
    public static final String PROP_ERROR_LISTENER = PFX + "callback.error";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see
     * {@link Builder#timeTraceLogger(TimeTraceLogger) timeTraceLogger}.
     */
    public static final String PROP_TIME_TRACE_LOGGER = PFX + "time.trace";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see
     * {@link Builder#statisticsCollector(StatisticsCollector) statisticsCollector}.
     */
    public static final String PROP_STATISTICS_COLLECTOR = PFX + "statisticscollector";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see {@link Builder#maxPingsOut(int) maxPingsOut}.
     */
    public static final String PROP_MAX_PINGS = PFX + "maxpings";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see {@link Builder#pingInterval(Duration)
     * pingInterval}.
     */
    public static final String PROP_PING_INTERVAL = PFX + "pinginterval";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see {@link Builder#requestCleanupInterval(Duration)
     * requestCleanupInterval}.
     */
    public static final String PROP_CLEANUP_INTERVAL = PFX + "cleanupinterval";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see
     * {@link Builder#connectionTimeout(Duration) connectionTimeout}.
     */
    public static final String PROP_CONNECTION_TIMEOUT = PFX + "timeout";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see
     * {@link Builder#socketReadTimeoutMillis(int) socketReadTimeoutMillis}.
     */
    public static final String PROP_SOCKET_READ_TIMEOUT_MS = PFX + "socket.read.timeout.ms";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see
     * {@link Builder#socketWriteTimeout(long) socketWriteTimeout}.
     */
    public static final String PROP_SOCKET_WRITE_TIMEOUT = PFX + "socket.write.timeout";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see
     * {@link Builder#socketSoLinger(int) socketSoLinger}.
     */
    public static final String PROP_SOCKET_SO_LINGER = PFX + "socket.so.linger";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see
     * {@link Builder#reconnectBufferSize(long) reconnectBufferSize}.
     */
    public static final String PROP_RECONNECT_BUF_SIZE = PFX + "reconnect.buffer.size";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see {@link Builder#reconnectWait(Duration)
     * reconnectWait}.
     */
    public static final String PROP_RECONNECT_WAIT = PFX + "reconnect.wait";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see {@link Builder#maxReconnects(int)
     * maxReconnects}.
     */
    public static final String PROP_MAX_RECONNECT = PFX + "reconnect.max";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see {@link Builder#reconnectJitter(Duration)
     * reconnectJitter}.
     */
    public static final String PROP_RECONNECT_JITTER = PFX + "reconnect.jitter";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see {@link Builder#reconnectJitterTls(Duration)
     * reconnectJitterTls}.
     */
    public static final String PROP_RECONNECT_JITTER_TLS = PFX + "reconnect.jitter.tls";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see {@link Builder#pedantic() pedantic}.
     */
    public static final String PROP_PEDANTIC = PFX + "pedantic";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see {@link Builder#verbose() verbose}.
     */
    public static final String PROP_VERBOSE = PFX + "verbose";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see {@link Builder#noEcho() noEcho}.
     */
    public static final String PROP_NO_ECHO = PFX + "noecho";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see {@link Builder#noHeaders() noHeaders}.
     */
    public static final String PROP_NO_HEADERS = PFX + "noheaders";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see {@link Builder#connectionName(String)
     * connectionName}.
     */
    public static final String PROP_CONNECTION_NAME = PFX + "name";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see {@link Builder#noNoResponders() noNoResponders}.
     */
    public static final String PROP_NO_NORESPONDERS = PFX + "nonoresponders";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see {@link Builder#noRandomize() noRandomize}.
     */
    public static final String PROP_NORANDOMIZE = PFX + "norandomize";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see {@link Builder#noResolveHostnames() noResolveHostnames}.
     */
    public static final String PROP_NO_RESOLVE_HOSTNAMES = PFX + "noResolveHostnames";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see {@link Builder#reportNoResponders() reportNoResponders}.
     */
    public static final String PROP_REPORT_NO_RESPONDERS = PFX + "reportNoResponders";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see {@link #clientSideLimitChecks() clientSideLimitChecks}.
     */
    public static final String PROP_CLIENT_SIDE_LIMIT_CHECKS = PFX + "clientsidelimitchecks";
    /**
     * Property used to configure a builder from a Properties object. {@value},
     * see {@link Builder#servers(String[]) servers}. The value can be a comma-separated list of server URLs.
     */
    public static final String PROP_SERVERS = PFX + "servers";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see {@link Builder#userInfo(String, String)
     * userInfo}.
     */
    public static final String PROP_PASSWORD = PFX + "password";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see {@link Builder#userInfo(String, String)
     * userInfo}.
     */
    public static final String PROP_USERNAME = PFX + "username";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see {@link Builder#token(String) token}.
     */
    public static final String PROP_TOKEN = PFX + "token";
    /**
     * Property used to configure the token supplier from a Properties object. {@value}, see {@link Builder#tokenSupplier(Supplier) tokenSupplier}.
     */
    public static final String PROP_TOKEN_SUPPLIER = PFX + "token.supplier";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see {@link Builder#server(String) server}.
     */
    public static final String PROP_URL = PFX + "url";
    /**
     * Property used to configure a builder from a Properties object. {@value},
     *  see {@link Builder#sslContext(SSLContext) sslContext}.
     * This property is a boolean flag, but it tells the options parser to use the
     * default SSL context. Set the default context before creating the options.
     */
    public static final String PROP_SECURE = PFX + "secure";
    /**
     * Property used to configure a builder from a Properties object.
     * {@value}, see {@link Builder#sslContext(SSLContext) sslContext}.
     * This property is a boolean flag, but it tells the options parser to use
     * an SSL context that takes any server TLS certificate and does not provide
     * its own. The server must have tls_verify turned OFF for this option to work.
     */
    public static final String PROP_OPENTLS = PFX + "opentls";
    /**
     * Property used to configure a builder from a Properties object.
     * {@value}, see {@link Builder#maxMessagesInOutgoingQueue(int) maxMessagesInOutgoingQueue}.
     */
    public static final String PROP_MAX_MESSAGES_IN_OUTGOING_QUEUE = PFX + "outgoingqueue.maxmessages";
    /**
     * Property used to configure a builder from a Properties object.
     * {@value}, see {@link Builder#discardMessagesWhenOutgoingQueueFull()
     * discardMessagesWhenOutgoingQueueFull}.
     */
    public static final String PROP_DISCARD_MESSAGES_WHEN_OUTGOING_QUEUE_FULL = PFX + "outgoingqueue.discardwhenfull";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see {@link Builder#oldRequestStyle()
     * oldRequestStyle}.
     */
    public static final String PROP_USE_OLD_REQUEST_STYLE = "use.old.request.style";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see {@link Builder#maxControlLine(int)
     * maxControlLine}.
     */
    public static final String PROP_MAX_CONTROL_LINE = "max.control.line";
    /**
     * Property used to set the inbox prefix
     */
    public static final String PROP_INBOX_PREFIX = "inbox.prefix";
    /**
     * Property used to set whether to ignore discovered servers when connecting
     */
    public static final String PROP_IGNORE_DISCOVERED_SERVERS = "ignore_discovered_servers";
    /**
     * Preferred property used to set whether to ignore discovered servers when connecting
     */
    public static final String PROP_IGNORE_DISCOVERED_SERVERS_PREFERRED = "ignore.discovered.servers";
    /**
     * Property used to set class name for ServerPool implementation
     * {@link Builder#serverPool(ServerPool) serverPool}.
     */
    public static final String PROP_SERVERS_POOL_IMPLEMENTATION_CLASS = "servers_pool_implementation_class";
    /**
     * Preferred property used to set class name for ServerPool implementation
     * {@link Builder#serverPool(ServerPool) serverPool}.
     */
    public static final String PROP_SERVERS_POOL_IMPLEMENTATION_CLASS_PREFERRED = "servers.pool.implementation.class";
    /**
     * Property used to set class name for the Dispatcher Factory
     * {@link Builder#dispatcherFactory(DispatcherFactory) dispatcherFactory}.
     */
    public static final String PROP_DISPATCHER_FACTORY_CLASS = "dispatcher.factory.class";
    /**
     * Property used to set class name for the SSLContextFactory
     * {@link Builder#sslContextFactory(SSLContextFactory) sslContextFactory}.
     */
    public static final String PROP_SSL_CONTEXT_FACTORY_CLASS = "ssl.context.factory.class";
    /**
     * Property for the keystore path used to create an SSLContext
     */
    public static final String PROP_KEYSTORE = PFX + "keyStore";
    /**
     * Property for the keystore password used to create an SSLContext
     */
    public static final String PROP_KEYSTORE_PASSWORD = PFX + "keyStorePassword";
    /**
     * Property for the truststore path used to create an SSLContext
     */
    public static final String PROP_TRUSTSTORE = PFX + "trustStore";
    /**
     * Property for the truststore password used to create an SSLContext
     */
    public static final String PROP_TRUSTSTORE_PASSWORD = PFX + "trustStorePassword";
    /**
     * Property for the algorithm used to create an SSLContext
     */
    public static final String PROP_TLS_ALGORITHM = PFX + "tls.algorithm";
    /**
     * Property used to set the path to a credentials file to be used in a FileAuthHandler
     */
    public static final String PROP_CREDENTIAL_PATH = PFX + "credential.path";
    /**
     * Property used to configure tls first behavior
     * This property is a boolean flag, telling connections whether
     * to do TLS upgrade first, before INFO
     */
    public static final String PROP_TLS_FIRST = PFX + "tls.first";
    /**
     * This property is used to enable support for UTF8 subjects. See {@link Builder#supportUTF8Subjects() supportUTF8Subjects()}
     */
    public static final String PROP_UTF8_SUBJECTS = "allow.utf8.subjects";
    /**
     * Property used to throw {@link java.util.concurrent.TimeoutException} on timeout instead of {@link java.util.concurrent.CancellationException}.
     * {@link Builder#useTimeoutException()}.
     */
    public static final String PROP_USE_TIMEOUT_EXCEPTION = PFX + "use.timeout.exception";
    /**
     * Property used to a dispatcher that dispatches messages via the executor service instead of with a blocking call.
     * {@link Builder#useDispatcherWithExecutor()}.
     */
    public static final String PROP_USE_DISPATCHER_WITH_EXECUTOR = PFX + "use.dispatcher.with.executor";
    /**
     * Property used to configure a builder from a Properties object. {@value}, see {@link #forceFlushOnRequest() forceFlushOnRequest}.
     */
    public static final String PROP_FORCE_FLUSH_ON_REQUEST = PFX + "force.flush.on.request";
    /**
     * Property used to set class name for the Executor Service (executor) class
     * {@link Builder#executor(ExecutorService) executor}.
     */
    public static final String PROP_EXECUTOR_SERVICE_CLASS = "executor.service.class";
    /**
     * Property used to set class name for the Executor Service (executor) class
     * {@link Builder#executor(ExecutorService) executor}.
     */
    public static final String PROP_SCHEDULED_EXECUTOR_SERVICE_CLASS = "scheduled.executor.service.class";
    /**
     * Property used to set class name for the Connect Thread Factory
     * {@link Builder#connectThreadFactory(ThreadFactory) connectThreadFactory}.
     */
    public static final String PROP_CONNECT_THREAD_FACTORY_CLASS = "connect.thread.factory.class";
    /**
     * Property used to set class name for the Callback Thread Factory
     * {@link Builder#callbackThreadFactory(ThreadFactory) callbackThreadFactory}.
     */
    public static final String PROP_CALLBACK_THREAD_FACTORY_CLASS = "callback.thread.factory.class";
    /**
     * Property used to set class name for the ReaderListener implementation
     * {@link Builder#readListener(ReadListener) readListener}.
     */
    public static final String PROP_READ_LISTENER_CLASS = "read.listener.class";

    /**
     * Property used to enable fast fallback algorithm for socket connection.
     * {@link Builder#enableFastFallback() enableFastFallback}.
     */
    public static final String PROP_FAST_FALLBACK = PFX + "fast.fallback";

    // ----------------------------------------------------------------------------------------------------
    // PROTOCOL CONNECT OPTION CONSTANTS
    // ----------------------------------------------------------------------------------------------------
    /**
     * Protocol key {@value}, see {@link Builder#verbose() verbose}.
     */
    static final String OPTION_VERBOSE = "verbose";

    /**
     * Protocol key {@value}, see {@link Builder#pedantic()
     * pedantic}.
     */
    static final String OPTION_PEDANTIC = "pedantic";

    /**
     * Protocol key {@value}, see
     * {@link Builder#sslContext(SSLContext) sslContext}.
     */
    static final String OPTION_TLS_REQUIRED = "tls_required";

    /**
     * Protocol key {@value}, see {@link Builder#token(String)
     * token}.
     */
    static final String OPTION_AUTH_TOKEN = "auth_token";

    /**
     * Protocol key {@value}, see
     * {@link Builder#userInfo(String, String) userInfo}.
     */
    static final String OPTION_USER = "user";

    /**
     * Protocol key {@value}, see
     * {@link Builder#userInfo(String, String) userInfo}.
     */
    static final String OPTION_PASSWORD = "pass";

    /**
     * Protocol key {@value}, see {@link Builder#connectionName(String)
     * connectionName}.
     */
    static final String OPTION_NAME = "name";

    /**
     * Protocol key {@value}, will be set to "Java".
     */
    static final String OPTION_LANG = "lang";

    /**
     * Protocol key {@value}, will be set to
     * {@link Nats#CLIENT_VERSION CLIENT_VERSION}.
     */
    static final String OPTION_VERSION = "version";

    /**
     * Protocol key {@value}, will be set to 1.
     */
    static final String OPTION_PROTOCOL = "protocol";

    /**
     * Echo key {@value}, determines if the server should echo to the client.
     */
    static final String OPTION_ECHO = "echo";

    /**
     * NKey key {@value}, the public key being used for sign-in.
     */
    static final String OPTION_NKEY = "nkey";

    /**
     * SIG key {@value}, the signature of the nonce sent by the server.
     */
    static final String OPTION_SIG = "sig";

    /**
     * JWT key {@value}, the user JWT to send to the server.
     */
    static final String OPTION_JWT = "jwt";

    /**
     * Headers key if headers are supported
     */
    static final String OPTION_HEADERS = "headers";

    /**
     * No Responders key if noresponders are supported
     */
    static final String OPTION_NORESPONDERS = "no_responders";

    // ----------------------------------------------------------------------------------------------------
    // CLASS VARIABLES
    // ----------------------------------------------------------------------------------------------------
    private final List<NatsUri> natsServerUris;
    private final List<String> unprocessedServers;
    private final boolean noRandomize;
    private final boolean noResolveHostnames;
    private final boolean reportNoResponders;
    private final String connectionName;
    private final boolean verbose;
    private final boolean pedantic;
    private final SSLContext sslContext;
    private final int maxReconnect;
    private final int maxControlLine;
    private final Duration reconnectWait;
    private final Duration reconnectJitter;
    private final Duration reconnectJitterTls;
    private final Duration connectionTimeout;
    private final int socketReadTimeoutMillis;
    private final Duration socketWriteTimeout;
    private final int socketSoLinger;
    private final Duration pingInterval;
    private final Duration requestCleanupInterval;
    private final int maxPingsOut;
    private final long reconnectBufferSize;
    private final char[] username;
    private final char[] password;
    private final Supplier<char[]> tokenSupplier;
    private final String inboxPrefix;
    private boolean useOldRequestStyle;
    private final int bufferSize;
    private final boolean noEcho;
    private final boolean noHeaders;
    private final boolean noNoResponders;
    private final boolean clientSideLimitChecks;
    private final boolean supportUTF8Subjects;
    private final int maxMessagesInOutgoingQueue;
    private final boolean discardMessagesWhenOutgoingQueueFull;
    private final boolean ignoreDiscoveredServers;
    private final boolean tlsFirst;
    private final boolean useTimeoutException;
    private final boolean useDispatcherWithExecutor;
    private final boolean forceFlushOnRequest;

    private final AuthHandler authHandler;
    private final ReconnectDelayHandler reconnectDelayHandler;

    private final ErrorListener errorListener;
    private final TimeTraceLogger timeTraceLogger;
    private final ConnectionListener connectionListener;
    private final ReadListener readListener;
    private final StatisticsCollector statisticsCollector;
    private final String dataPortType;

    private final boolean trackAdvancedStats;
    private final boolean traceConnection;

    private final ExecutorService executor;
    private final ScheduledExecutorService scheduledExecutor;
    private final ThreadFactory connectThreadFactory;
    private final ThreadFactory callbackThreadFactory;
    private final ServerPool serverPool;
    private final DispatcherFactory dispatcherFactory;

    private final List<java.util.function.Consumer<HttpRequest>> httpRequestInterceptors;
    private final Proxy proxy;
    private final boolean enableFastFallback;

    static class DefaultThreadFactory implements ThreadFactory {
        String name;
        AtomicInteger threadNo = new AtomicInteger(0);

        public DefaultThreadFactory (String name){
            this.name = name;
        }

        public Thread newThread(Runnable r) {
            String threadName = name+":"+threadNo.incrementAndGet();
            Thread t = new Thread(r,threadName);
            if (t.isDaemon()) {
                t.setDaemon(false);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }

    static class DefaultTokenSupplier implements Supplier<char[]> {
        final char[] token;

        public DefaultTokenSupplier() {
            token = null;
        }

        public DefaultTokenSupplier(char[] token) {
            this.token = token == null || token.length == 0 ? null : token;
        }

        public DefaultTokenSupplier(String token) {
            token = Validator.emptyAsNull(token);
            this.token = token == null ? null : token.toCharArray();
        }

        @Override
        public char[] get() {
            return token;
        }
    }

    /**
     * Set old request style.
     * @param value true to use the old request style
     * @deprecated Use Builder
     */
    @Deprecated
    public void setOldRequestStyle(boolean value) {
        useOldRequestStyle = value;
    }

    // ----------------------------------------------------------------------------------------------------
    // BUILDER
    // ----------------------------------------------------------------------------------------------------
    /**
     * Creates a builder for the options in a fluent style
     * @return the builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Options are created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls. The builder can also
     * be created from a properties object using the property names defined with the
     * prefix PROP_ in this class.
     * <p>A common usage for testing might be {@code new Options.Builder().server(myserverurl).noReconnect.build()}
     */
    public static class Builder {

        // ----------------------------------------------------------------------------------------------------
        // BUILDER VARIABLES
        // ----------------------------------------------------------------------------------------------------
        private final List<NatsUri> natsServerUris = new ArrayList<>();
        private final List<String> unprocessedServers = new ArrayList<>();
        private boolean noRandomize = false;
        private boolean noResolveHostnames = false;
        private boolean reportNoResponders = false;
        private String connectionName = null; // Useful for debugging -> "test: " + NatsTestServer.currentPort();
        private boolean verbose = false;
        private boolean pedantic = false;
        private SSLContext sslContext = null;
        private SSLContextFactory sslContextFactory = null;
        private int maxControlLine = DEFAULT_MAX_CONTROL_LINE;
        private int maxReconnect = DEFAULT_MAX_RECONNECT;
        private Duration reconnectWait = DEFAULT_RECONNECT_WAIT;
        private Duration reconnectJitter = DEFAULT_RECONNECT_JITTER;
        private Duration reconnectJitterTls = DEFAULT_RECONNECT_JITTER_TLS;
        private Duration connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
        private int socketReadTimeoutMillis = 0;
        private Duration socketWriteTimeout = DEFAULT_SOCKET_WRITE_TIMEOUT;
        private int socketSoLinger = -1;
        private Duration pingInterval = DEFAULT_PING_INTERVAL;
        private Duration requestCleanupInterval = DEFAULT_REQUEST_CLEANUP_INTERVAL;
        private int maxPingsOut = DEFAULT_MAX_PINGS_OUT;
        private long reconnectBufferSize = DEFAULT_RECONNECT_BUF_SIZE;
        private char[] username = null;
        private char[] password = null;
        private Supplier<char[]> tokenSupplier = new DefaultTokenSupplier();
        private boolean useOldRequestStyle = false;
        private int bufferSize = DEFAULT_BUFFER_SIZE;
        private boolean trackAdvancedStats = false;
        private boolean traceConnection = false;
        private boolean noEcho = false;
        private boolean noHeaders = false;
        private boolean noNoResponders = false;
        private boolean clientSideLimitChecks = true;
        private boolean supportUTF8Subjects = false;
        private String inboxPrefix = DEFAULT_INBOX_PREFIX;
        private int maxMessagesInOutgoingQueue = DEFAULT_MAX_MESSAGES_IN_OUTGOING_QUEUE;
        private boolean discardMessagesWhenOutgoingQueueFull = DEFAULT_DISCARD_MESSAGES_WHEN_OUTGOING_QUEUE_FULL;
        private boolean ignoreDiscoveredServers = false;
        private boolean tlsFirst = false;
        private boolean useTimeoutException = false;
        private boolean useDispatcherWithExecutor = false;
        private boolean forceFlushOnRequest = true; // true since it's the original b/w compatible way
        private ServerPool serverPool = null;
        private DispatcherFactory dispatcherFactory = null;

        private AuthHandler authHandler;
        private ReconnectDelayHandler reconnectDelayHandler;

        private ErrorListener errorListener = null;
        private TimeTraceLogger timeTraceLogger = null;
        private ConnectionListener connectionListener = null;
        private ReadListener readListener = null;
        private StatisticsCollector statisticsCollector = null;
        private String dataPortType = DEFAULT_DATA_PORT_TYPE;
        private ExecutorService executor;
        private ScheduledExecutorService scheduledExecutor;
        private ThreadFactory connectThreadFactory;
        private ThreadFactory callbackThreadFactory;
        private List<java.util.function.Consumer<HttpRequest>> httpRequestInterceptors;
        private Proxy proxy;

        private boolean useDefaultTls;
        private boolean useTrustAllTls;
        private String keystore;
        private char[] keystorePassword;
        private String truststore;
        private char[] truststorePassword;
        private String tlsAlgorithm = DEFAULT_TLS_ALGORITHM;
        private String credentialPath;
        private boolean enableFastFallback = false;

        /**
         * Constructs a new Builder with the default values.
         * <p>When {@link #build() build()} is called on a default builder it will add the {@link Options#DEFAULT_URL
         * default url} to its list of servers if there were no servers defined.</p>
         */
        public Builder() {}

        // ----------------------------------------------------------------------------------------------------
        // BUILD CONSTRUCTOR PROPS
        // ----------------------------------------------------------------------------------------------------
        /**
         * Constructs a new {@code Builder} from a {@link Properties} object.
         * <p>Methods called on the builder after construction can override the properties.</p>
         * @param props the {@link Properties} object
         */
        public Builder(Properties props) throws IllegalArgumentException {
            properties(props);
        }

        /**
         * Constructs a new {@code Builder} from a file that contains properties.
         * @param propertiesFilePath a resolvable path to a file from the location the application is running, either relative or absolute
         * @throws IOException if the properties file cannot be found, opened or read
         */
        public Builder(String propertiesFilePath) throws IOException {
            Properties props = new Properties();
            props.load(Files.newInputStream(Paths.get(propertiesFilePath)));
            properties(props);
        }

        // ----------------------------------------------------------------------------------------------------
        // BUILDER METHODS
        // ----------------------------------------------------------------------------------------------------

        /**
         * Add settings defined in the properties object
         * @param props the properties object
         * @throws IllegalArgumentException if the properties object is null
         * @return the Builder for chaining
         */
        public Builder properties(Properties props) {
            if (props == null) {
                throw new IllegalArgumentException("Properties cannot be null");
            }
            stringProperty(props, PROP_URL, this::server);
            stringProperty(props, PROP_SERVERS, str -> {
                String[] servers = str.trim().split(",\\s*");
                this.servers(servers);
            });

            charArrayProperty(props, PROP_USERNAME, ca -> this.username = ca);
            charArrayProperty(props, PROP_PASSWORD, ca -> this.password = ca);
            charArrayProperty(props, PROP_TOKEN, ca -> this.tokenSupplier = new DefaultTokenSupplier(ca));
            //noinspection unchecked
            classnameProperty(props, PROP_TOKEN_SUPPLIER, o -> this.tokenSupplier = (Supplier<char[]>) o);

            booleanProperty(props, PROP_SECURE, b -> this.useDefaultTls = b);
            booleanProperty(props, PROP_OPENTLS, b -> this.useTrustAllTls = b);

            classnameProperty(props, PROP_SSL_CONTEXT_FACTORY_CLASS, o -> this.sslContextFactory = (SSLContextFactory) o);
            stringProperty(props, PROP_KEYSTORE, s -> this.keystore = s);
            charArrayProperty(props, PROP_KEYSTORE_PASSWORD, ca -> this.keystorePassword = ca);
            stringProperty(props, PROP_TRUSTSTORE, s -> this.truststore = s);
            charArrayProperty(props, PROP_TRUSTSTORE_PASSWORD, ca -> this.truststorePassword = ca);
            stringProperty(props, PROP_TLS_ALGORITHM, s -> this.tlsAlgorithm = s);

            stringProperty(props, PROP_CREDENTIAL_PATH, s -> this.credentialPath = s);

            stringProperty(props, PROP_CONNECTION_NAME, s -> this.connectionName = s);

            booleanProperty(props, PROP_NORANDOMIZE, b -> this.noRandomize = b);
            booleanProperty(props, PROP_NO_RESOLVE_HOSTNAMES, b -> this.noResolveHostnames = b);
            booleanProperty(props, PROP_REPORT_NO_RESPONDERS, b -> this.reportNoResponders = b);

            stringProperty(props, PROP_CONNECTION_NAME, s -> this.connectionName = s);
            booleanProperty(props, PROP_VERBOSE, b -> this.verbose = b);
            booleanProperty(props, PROP_NO_ECHO, b -> this.noEcho = b);
            booleanProperty(props, PROP_NO_HEADERS, b -> this.noHeaders = b);
            booleanProperty(props, PROP_NO_NORESPONDERS, b -> this.noNoResponders = b);
            booleanProperty(props, PROP_CLIENT_SIDE_LIMIT_CHECKS, b -> this.clientSideLimitChecks = b);
            booleanProperty(props, PROP_UTF8_SUBJECTS, b -> this.supportUTF8Subjects = b);
            booleanProperty(props, PROP_PEDANTIC, b -> this.pedantic = b);

            intProperty(props, PROP_MAX_RECONNECT, DEFAULT_MAX_RECONNECT, i -> this.maxReconnect = i);
            durationProperty(props, PROP_RECONNECT_WAIT, DEFAULT_RECONNECT_WAIT, d -> this.reconnectWait = d);
            durationProperty(props, PROP_RECONNECT_JITTER, DEFAULT_RECONNECT_JITTER, d -> this.reconnectJitter = d);
            durationProperty(props, PROP_RECONNECT_JITTER_TLS, DEFAULT_RECONNECT_JITTER_TLS, d -> this.reconnectJitterTls = d);
            longProperty(props, PROP_RECONNECT_BUF_SIZE, DEFAULT_RECONNECT_BUF_SIZE, l -> this.reconnectBufferSize = l);
            durationProperty(props, PROP_CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT, d -> this.connectionTimeout = d);
            intProperty(props, PROP_SOCKET_READ_TIMEOUT_MS, -1, i -> this.socketReadTimeoutMillis = i);
            durationProperty(props, PROP_SOCKET_WRITE_TIMEOUT, DEFAULT_SOCKET_WRITE_TIMEOUT, d -> this.socketWriteTimeout = d);
            intProperty(props, PROP_SOCKET_SO_LINGER, -1, i -> socketSoLinger = i);

            intGtEqZeroProperty(props, PROP_MAX_CONTROL_LINE, DEFAULT_MAX_CONTROL_LINE, i -> this.maxControlLine = i);
            durationProperty(props, PROP_PING_INTERVAL, DEFAULT_PING_INTERVAL, d -> this.pingInterval = d);
            durationProperty(props, PROP_CLEANUP_INTERVAL, DEFAULT_REQUEST_CLEANUP_INTERVAL, d -> this.requestCleanupInterval = d);
            intProperty(props, PROP_MAX_PINGS, DEFAULT_MAX_PINGS_OUT, i -> this.maxPingsOut = i);
            booleanProperty(props, PROP_USE_OLD_REQUEST_STYLE, b -> this.useOldRequestStyle = b);

            classnameProperty(props, PROP_ERROR_LISTENER, o -> this.errorListener = (ErrorListener) o);
            classnameProperty(props, PROP_TIME_TRACE_LOGGER, o -> this.timeTraceLogger = (TimeTraceLogger) o);
            classnameProperty(props, PROP_CONNECTION_CB, o -> this.connectionListener = (ConnectionListener) o);
            classnameProperty(props, PROP_READ_LISTENER_CLASS, o -> this.readListener = (ReadListener) o);
            classnameProperty(props, PROP_STATISTICS_COLLECTOR, o -> this.statisticsCollector = (StatisticsCollector) o);

            stringProperty(props, PROP_DATA_PORT_TYPE, s -> this.dataPortType = s);
            stringProperty(props, PROP_INBOX_PREFIX, this::inboxPrefix);
            intGtEqZeroProperty(props, PROP_MAX_MESSAGES_IN_OUTGOING_QUEUE, DEFAULT_MAX_MESSAGES_IN_OUTGOING_QUEUE, i -> this.maxMessagesInOutgoingQueue = i);
            booleanProperty(props, PROP_DISCARD_MESSAGES_WHEN_OUTGOING_QUEUE_FULL, b -> this.discardMessagesWhenOutgoingQueueFull = b);

            booleanProperty(props, PROP_IGNORE_DISCOVERED_SERVERS, b -> this.ignoreDiscoveredServers = b);
            booleanProperty(props, PROP_TLS_FIRST, b -> this.tlsFirst = b);
            booleanProperty(props, PROP_USE_TIMEOUT_EXCEPTION, b -> this.useTimeoutException = b);
            booleanProperty(props, PROP_USE_DISPATCHER_WITH_EXECUTOR, b -> this.useDispatcherWithExecutor = b);
            booleanProperty(props, PROP_FORCE_FLUSH_ON_REQUEST, b -> this.forceFlushOnRequest = b);
            booleanProperty(props, PROP_FAST_FALLBACK, b -> this.enableFastFallback = b);

            classnameProperty(props, PROP_SERVERS_POOL_IMPLEMENTATION_CLASS, o -> this.serverPool = (ServerPool) o);
            classnameProperty(props, PROP_DISPATCHER_FACTORY_CLASS, o -> this.dispatcherFactory = (DispatcherFactory) o);
            classnameProperty(props, PROP_EXECUTOR_SERVICE_CLASS, o -> this.executor = (ExecutorService) o);
            classnameProperty(props, PROP_SCHEDULED_EXECUTOR_SERVICE_CLASS, o -> this.scheduledExecutor = (ScheduledExecutorService) o);
            classnameProperty(props, PROP_CONNECT_THREAD_FACTORY_CLASS, o -> this.connectThreadFactory = (ThreadFactory) o);
            classnameProperty(props, PROP_CALLBACK_THREAD_FACTORY_CLASS, o -> this.callbackThreadFactory = (ThreadFactory) o);
            return this;
        }

        /**
         * Add a server to the list of known servers.
         *
         * @param serverURL the URL for the server to add
         * @throws IllegalArgumentException if the url is not formatted correctly.
         * @return the Builder for chaining
         */
        public Builder server(String serverURL) {
            return servers(serverURL.trim().split(","));
        }

        /**
         * Add an array of servers to the list of known servers.
         *
         * @param servers A list of server URIs
         * @throws IllegalArgumentException if any url is not formatted correctly.
         * @return the Builder for chaining
         */
        public Builder servers(String[] servers) {
            for (String s : servers) {
                if (s != null && !s.isEmpty()) {
                    try {
                        String unprocessed = s.trim();
                        NatsUri nuri = new NatsUri(unprocessed);
                        if (!natsServerUris.contains(nuri)) {
                            natsServerUris.add(nuri);
                            unprocessedServers.add(unprocessed);
                        }
                    }
                    catch (URISyntaxException e) {
                        throw new IllegalArgumentException(e);
                    }
                }
            }
            return this;
        }

        /**
         * Turn on the old request style that uses a new inbox and subscriber for each
         * request.
         * @return the Builder for chaining
         */
        public Builder oldRequestStyle() {
            this.useOldRequestStyle = true;
            return this;
        }

        /**
         * For the default server list provider, turn off server pool randomization.
         * The default provider will pick servers from its list randomly on a reconnect.
         * When noRandomize is set to true the default provider supplies a list that
         * first contains servers as configured and then contains the servers as sent
         * from the connected server.
         * @return the Builder for chaining
         */
        public Builder noRandomize() {
            this.noRandomize = true;
            return this;
        }

        /**
         * For the default server list provider, whether to resolve hostnames when building server list.
         * @return the Builder for chaining
         */
        public Builder noResolveHostnames() {
            this.noResolveHostnames = true;
            return this;
        }

        public Builder reportNoResponders() {
            this.reportNoResponders = true;
            return this;
        }

        /**
         * Turn off echo. If supported by the nats-server version you are connecting to this
         * flag will prevent the server from echoing messages back to the connection if it
         * has subscriptions on the subject being published to.
         * @return the Builder for chaining
         */
        public Builder noEcho() {
            this.noEcho = true;
            return this;
        }

        /**
         * Turn off header support. Some versions of the server don't support it.
         * It's also not required if you don't use headers
         * @return the Builder for chaining
         */
        public Builder noHeaders() {
            this.noHeaders = true;
            return this;
        }

        /**
         * Turn off noresponder support. Some versions of the server don't support it.
         * @return the Builder for chaining
         */
        public Builder noNoResponders() {
            this.noNoResponders = true;
            return this;
        }

        /**
         * Set client side limit checks. Default is true
         * @param checks the checks flag
         * @return the Builder for chaining
         */
        public Builder clientSideLimitChecks(boolean checks) {
            this.clientSideLimitChecks = checks;
            return this;
        }

        /**
         * The client protocol is not clear about the encoding for subject names. For
         * performance reasons, the Java client defaults to ASCII. You can enable UTF8
         * with this method. The server, written in go, treats byte to string as UTF8 by default
         * and should allow UTF8 subjects, but make sure to test any clients when using them.
         * @return the Builder for chaining
         */
        public Builder supportUTF8Subjects() {
            this.supportUTF8Subjects = true;
            return this;
        }

        /**
         * Set the connection's optional Name.
         *
         * @param name the connections new name.
         * @return the Builder for chaining
         */
        public Builder connectionName(String name) {
            this.connectionName = name;
            return this;
        }

        /**
         * Set the connection's inbox prefix. All inboxes will start with this string.
         *
         * @param prefix prefix to use.
         * @return the Builder for chaining
         */
        public Builder inboxPrefix(String prefix) {
            this.inboxPrefix = prefix;

            if (!this.inboxPrefix.endsWith(".")) {
                this.inboxPrefix = this.inboxPrefix + ".";
            }
            return this;
        }

        /**
         * Turn on verbose mode with the server.
         * @return the Builder for chaining
         */
        public Builder verbose() {
            this.verbose = true;
            return this;
        }

        /**
         * Turn on pedantic mode for the server, in relation to this connection.
         * @return the Builder for chaining
         */
        public Builder pedantic() {
            this.pedantic = true;
            return this;
        }

        /**
         * Turn on advanced stats, primarily for test/benchmarks. These are visible if you
         * call toString on the {@link Statistics Statistics} object.
         * @return the Builder for chaining
         */
        public Builder turnOnAdvancedStats() {
            this.trackAdvancedStats = true;
            return this;
        }

        /**
         * Enable connection trace messages. Messages are printed to standard out. This option is for very
         * fine-grained debugging of connection issues.
         * @return the Builder for chaining
         */
        public Builder traceConnection() {
            this.traceConnection = true;
            return this;
        }

        /**
         * Sets the options to use the default SSL Context, if it exists.
         * @throws NoSuchAlgorithmException <em>Not thrown, deferred to build() method, left in for backward compatibility</em>
         * @return the Builder for chaining
         */
        public Builder secure() throws NoSuchAlgorithmException {
            useDefaultTls = true;
            return this;
        }

        /**
         * Set the options to use an SSL context that accepts any server certificate and has no client certificates.
         * @throws NoSuchAlgorithmException <em>Not thrown, deferred to build() method, left in for backward compatibility</em>
         * @return the Builder for chaining
         */
        public Builder opentls() throws NoSuchAlgorithmException {
            useTrustAllTls = true;
            return this;
        }

        /**
         * Set the SSL context, requires that the server supports TLS connections and
         * the URI specifies TLS.
         * If provided, the context takes precedence over any other TLS/SSL properties
         * set in the builder, including the sslContextFactory
         * @param ctx the SSL Context to use for TLS connections
         * @return the Builder for chaining
         */
        public Builder sslContext(SSLContext ctx) {
            this.sslContext = ctx;
            return this;
        }

        /**
         * Set the factory that provides the ssl context. The factory is superseded
         * by an instance of SSLContext
         * @param sslContextFactory the SSL Context for use to create a ssl context
         * @return the Builder for chaining
         */
        public Builder sslContextFactory(SSLContextFactory sslContextFactory) {
            this.sslContextFactory = sslContextFactory;
            return this;
        }

        /**
         *
         * @param keystore the path to the keystore file
         * @return the Builder for chaining
         */
        public Builder keystorePath(String keystore) {
            this.keystore = emptyAsNull(keystore);
            return this;
        }

        /**
         *
         * @param keystorePassword the password for the keystore
         * @return the Builder for chaining
         */
        public Builder keystorePassword(char[] keystorePassword) {
            this.keystorePassword = keystorePassword == null || keystorePassword.length == 0 ? null : keystorePassword;
            return this;
        }

        /**
         *
         * @param truststore the path to the trust store file
         * @return the Builder for chaining
         */
        public Builder truststorePath(String truststore) {
            this.truststore = emptyAsNull(truststore);
            return this;
        }

        /**
         *
         * @param truststorePassword the password for the trust store
         * @return the Builder for chaining
         */
        public Builder truststorePassword(char[] truststorePassword) {
            this.truststorePassword = truststorePassword == null || truststorePassword.length == 0 ? null : truststorePassword;
            return this;
        }

        /**
         *
         * @param tlsAlgorithm the tls algorithm. Default is {@value SSLUtils#DEFAULT_TLS_ALGORITHM}
         * @return the Builder for chaining
         */
        public Builder tlsAlgorithm(String tlsAlgorithm) {
            this.tlsAlgorithm = emptyOrNullAs(tlsAlgorithm, DEFAULT_TLS_ALGORITHM);
            return this;
        }

        /**
         *
         * @param credentialPath the path to the credentials file for creating an {@link AuthHandler AuthHandler}
         * @return the Builder for chaining
         */
        public Builder credentialPath(String credentialPath) {
            this.credentialPath = emptyAsNull(credentialPath);
            return this;
        }

        /**
         * Equivalent to calling maxReconnects with 0, {@link #maxReconnects(int) maxReconnects}.
         * @return the Builder for chaining
         */
        public Builder noReconnect() {
            this.maxReconnect = 0;
            return this;
        }

        /**
         * Set the maximum number of reconnect attempts. Use 0 to turn off
         * auto-reconnect. Use -1 to turn on infinite reconnects.
         *
         * <p>The reconnect count is incremented on a per-server basis, so if the server list contains 5 servers
         * but max reconnects is set to 3, only 3 of those servers will be tried.</p>
         *
         * <p>This library has a slight difference from some NATS clients, if you set the maxReconnects to zero
         * there will not be any reconnect attempts, regardless of the number of known servers.</p>
         *
         * <p>The reconnect state is entered when the connection is connected and loses
         * that connection. During the initial connection attempt, the client will cycle over
         * its server list one time, regardless of what maxReconnects is set to. The only exception
         * to this is the async connect method {@link Nats#connectAsynchronously(Options, boolean) connectAsynchronously}.</p>
         *
         * @param max the maximum reconnect attempts
         * @return the Builder for chaining
         */
        public Builder maxReconnects(int max) {
            this.maxReconnect = max;
            return this;
        }

        /**
         * Set the time to wait between reconnect attempts to the same server. This setting is only used
         * by the client when the same server appears twice in the reconnect attempts, either because it is the
         * only known server or by random chance. Note, the randomization of the server list doesn't occur per
         * attempt, it is performed once at the start, so if there are 2 servers in the list you will never encounter
         * the reconnect wait.
         *
         * @param time the time to wait
         * @return the Builder for chaining
         */
        public Builder reconnectWait(Duration time) {
            this.reconnectWait = time;
            return this;
        }

        /**
         * Set the jitter time to wait between reconnect attempts to the same server. This setting is used to vary
         * the reconnect wait to avoid multiple clients trying to reconnect to servers at the same time.
         *
         * @param time the time to wait
         * @return the Builder for chaining
         */
        public Builder reconnectJitter(Duration time) {
            this.reconnectJitter = time;
            return this;
        }

        /**
         * Set the jitter time for a tls/secure connection to wait between reconnect attempts to the same server.
         * This setting is used to vary the reconnect wait to avoid multiple clients trying to reconnect to
         * servers at the same time.
         *
         * @param time the time to wait
         * @return the Builder for chaining
         */
        public Builder reconnectJitterTls(Duration time) {
            this.reconnectJitterTls = time;
            return this;
        }

        /**
         * Set the maximum length of a control line sent by this connection. This value is also configured
         * in the server but the protocol doesn't currently forward that setting. Configure it here so that
         * the client can ensure that messages are valid before sending to the server.
         *
         * @param bytes the max byte count
         * @return the Builder for chaining
         */
        public Builder maxControlLine(int bytes) {
            this.maxControlLine = bytes < 0 ? DEFAULT_MAX_CONTROL_LINE : bytes;
            return this;
        }

        /**
         * Set the timeout for connection attempts. Each server in the options is allowed this timeout
         * so if 3 servers are tried with a timeout of 5s the total time could be 15s.
         *
         * @param connectionTimeout the time to wait
         * @return the Builder for chaining
         */
        public Builder connectionTimeout(Duration connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        /**
         * Set the timeout for connection attempts. Each server in the options is allowed this timeout
         * so if 3 servers are tried with a timeout of 5s the total time could be 15s.
         *
         * @param connectionTimeoutMillis the time to wait in milliseconds
         * @return the Builder for chaining
         */
        public Builder connectionTimeout(long connectionTimeoutMillis) {
            this.connectionTimeout = Duration.ofMillis(connectionTimeoutMillis);
            return this;
        }

        /**
         * Set the timeout to use around socket reads
         * @param socketReadTimeoutMillis the timeout milliseconds
         * @return the Builder for chaining
         */
        public Builder socketReadTimeoutMillis(int socketReadTimeoutMillis) {
            this.socketReadTimeoutMillis = socketReadTimeoutMillis;
            return this;
        }

        /**
         * Set the timeout to use around socket writes
         * @param socketWriteTimeoutMillis the timeout milliseconds
         * @return the Builder for chaining
         */
        public Builder socketWriteTimeout(long socketWriteTimeoutMillis) {
            socketWriteTimeout = Duration.ofMillis(socketWriteTimeoutMillis);
            return this;
        }

        /**
         * Set the timeout to use around socket writes
         * @param socketWriteTimeout the timeout duration
         * @return the Builder for chaining
         */
        public Builder socketWriteTimeout(Duration socketWriteTimeout) {
            this.socketWriteTimeout = socketWriteTimeout;
            return this;
        }

        /**
         * Set the value of the socket SO LINGER property in seconds.
         * This feature is used by library data port implementations.
         * Setting this is a last resort if socket closes are a problem
         * in your environment, otherwise it's generally not necessary
         * to set this. The value must be greater than or equal to 0
         * to have the code call socket.setSoLinger with true and the timeout value
         * @param socketSoLinger the number of seconds to linger
         * @return the Builder for chaining
         */
        public Builder socketSoLinger(int socketSoLinger) {
            this.socketSoLinger = socketSoLinger;
            return this;
        }

        /**
         * Set the interval between attempts to pings the server. These pings are automated,
         * and capped by {@link #maxPingsOut(int) maxPingsOut()}. As of 2.4.4 the library
         * may wait up to 2 * time to send a ping. Incoming traffic from the server can postpone
         * the next ping to avoid pings taking up bandwidth during busy messaging.
         * Keep in mind that a ping requires a round trip to the server. Setting this value to a small
         * number can result in quick failures due to maxPingsOut being reached, these failures will
         * force a disconnect/reconnect which can result in messages being held back or failed. In general,
         * the ping interval should be set in seconds but this value is not enforced as it would result in
         * an API change from the 2.0 release.
         *
         * @param time the time between client to server pings
         * @return the Builder for chaining
         */
        public Builder pingInterval(Duration time) {
            this.pingInterval = time == null ? DEFAULT_PING_INTERVAL : time;
            return this;
        }

        /**
         * Set the interval between cleaning passes on outstanding request futures that are cancelled or timeout
         * in the application code.
         *
         * <p>The default value is probably reasonable, but this interval is useful in a very noisy network
         * situation where lots of requests are used.
         *
         * @param time the cleaning interval
         * @return the Builder for chaining
         */
        public Builder requestCleanupInterval(Duration time) {
            this.requestCleanupInterval = time;
            return this;
        }

        /**
         * Set the maximum number of pings the client can have in flight.
         *
         * @param max the max pings
         * @return the Builder for chaining
         */
        public Builder maxPingsOut(int max) {
            this.maxPingsOut = max;
            return this;
        }

        /**
         * Sets the initial size for buffers in the connection, primarily for testing.
         * @param size the size in bytes to make buffers for connections created with this options
         * @return the Builder for chaining
         */
        public Builder bufferSize(int size) {
            this.bufferSize = size;
            return this;
        }

        /**
         * Set the maximum number of bytes to buffer in the client when trying to
         * reconnect. When this value is exceeded the client will start to drop messages.
         * The count of dropped messages can be read from the {@link Statistics#getDroppedCount() Statistics}.
         * A value of zero will disable the reconnect buffer, a value less than zero means unlimited. Caution
         * should be used for negative numbers as they can result in an unreliable network connection plus a
         * high message rate leading to an out of memory error.
         *
         * @param size the size in bytes
         * @return the Builder for chaining
         */
        public Builder reconnectBufferSize(long size) {
            this.reconnectBufferSize = size;
            return this;
        }

        /**
         * Set the username and password for basic authentication.
         * If the user and password are set in the server URL, they will override these values. However, in a clustering situation,
         * these values can be used as a fallback.
         * use the char[] version instead for better security
         *
         * @param userName a non-empty userName
         * @param password the password, in plain text
         * @return the Builder for chaining
         */
        public Builder userInfo(String userName, String password) {
            this.username = userName.toCharArray();
            this.password = password.toCharArray();
            return this;
        }

        /**
         * Set the username and password for basic authentication.
         * If the user and password are set in the server URL, they will override these values. However, in a clustering situation,
         * these values can be used as a fallback.
         *
         * @param userName a non-empty userName
         * @param password the password, in plain text
         * @return the Builder for chaining
         */
        public Builder userInfo(char[] userName, char[] password) {
            this.username = userName;
            this.password = password;
            return this;
        }

        /**
         * Set the token for token-based authentication.
         * If a token is provided in a server URI, it overrides this value.
         *
         * @param token The token
         * @return the Builder for chaining
         * @deprecated use the char[] version instead for better security
         */
        @Deprecated
        public Builder token(String token) {
            this.tokenSupplier = new DefaultTokenSupplier(token);
            return this;
        }

        /**
         * Set the token for token-based authentication.
         * If a token is provided in a server URI, it overrides this value.
         *
         * @param token The token
         * @return the Builder for chaining
         */
        public Builder token(char[] token) {
            this.tokenSupplier = new DefaultTokenSupplier(token);
            return this;
        }

        /**
         * Set the token supplier for token-based authentication.
         * If a token is provided in a server URI, it overrides this value.
         *
         * @param tokenSupplier The tokenSupplier
         * @return the Builder for chaining
         */
        public Builder tokenSupplier(Supplier<char[]> tokenSupplier) {
            this.tokenSupplier = tokenSupplier == null ? new DefaultTokenSupplier() : tokenSupplier;
            return this;
        }

        /**
         * Set the {@link AuthHandler AuthHandler} to sign the server nonce for authentication in
         * nonce-mode.
         *
         * @param handler The new AuthHandler for this connection.
         * @return the Builder for chaining
         */
        public Builder authHandler(AuthHandler handler) {
            this.authHandler = handler;
            return this;
        }

        /**
         * Set the {@link ReconnectDelayHandler ReconnectDelayHandler} for custom reconnect duration
         *
         * @param handler The new ReconnectDelayHandler for this connection.
         * @return the Builder for chaining
         */
        public Builder reconnectDelayHandler(ReconnectDelayHandler handler) {
            this.reconnectDelayHandler = handler;
            return this;
        }

        /**
         * Set the {@link ErrorListener ErrorListener} to receive asynchronous error events related to this
         * connection.
         *
         * @param listener The new ErrorListener for this connection.
         * @return the Builder for chaining
         */
        public Builder errorListener(ErrorListener listener) {
            this.errorListener = listener;
            return this;
        }

        /**
         * Set the {@link TimeTraceLogger TimeTraceLogger} to receive trace events related to this connection.
         * @param logger The new TimeTraceLogger for this connection.
         * @return the Builder for chaining
         */
        public Builder timeTraceLogger(TimeTraceLogger logger) {
            this.timeTraceLogger = logger;
            return this;
        }

        /**
         * Set the {@link ConnectionListener ConnectionListener} to receive asynchronous notifications of disconnect
         * events.
         *
         * @param listener The new ConnectionListener for this type of event.
         * @return the Builder for chaining
         */
        public Builder connectionListener(ConnectionListener listener) {
            this.connectionListener = listener;
            return this;
        }

        /**
         * Sets a listener to be notified on incoming protocol/message
         *
         * @param readListener the listener
         * @return the Builder for chaining
         */
        public Builder readListener(ReadListener readListener) {
            this.readListener = readListener;
            return this;
        }

        /**
         * Set the {@link StatisticsCollector StatisticsCollector} to collect connection metrics.
         * <p>
         * If not set, then a default implementation will be used.
         *
         * @param collector the new StatisticsCollector for this connection.
         * @return the Builder for chaining
         */
        public Builder statisticsCollector(StatisticsCollector collector) {
            this.statisticsCollector = collector;
            return this;
        }

        /**
         * Set the {@link ExecutorService ExecutorService} used to run threaded tasks. The default is a
         * cached thread pool that names threads after the connection name (or a default). This executor
         * is used for reading and writing the underlying sockets as well as for each Dispatcher.
         * The default executor uses a short keepalive time, 500ms, to insure quick shutdowns. This is reasonable
         * since most threads from the executor are long-lived. If you customize, be sure to keep the shutdown
         * effect in mind, executors can block for their keepalive time. The default executor also marks threads
         * with priority normal and as non-daemon.
         *
         * @param executor The ExecutorService to use for connections built with these options.
         * @return the Builder for chaining
         */
        public Builder executor(ExecutorService executor) {
            this.executor = executor;
            return this;
        }

        /**
         * Set the {@link ScheduledExecutorService ScheduledExecutorService} used to run scheduled task like
         * heartbeat timers
         * The default is a ScheduledThreadPoolExecutor that does not
         *  execute delayed tasks after shutdown and removes tasks on cancel;
         * @param scheduledExecutor The ScheduledExecutorService to use for timer tasks
         * @return the Builder for chaining
         */
        public Builder scheduledExecutor(ScheduledExecutorService scheduledExecutor) {
            this.scheduledExecutor = scheduledExecutor;
            return this;
        }

        /**
         * Sets custom thread factory for the executor service
         *
         * @param threadFactory the thread factory to use for the executor service
         * @return the Builder for chaining
         */
        public Builder connectThreadFactory(ThreadFactory threadFactory) {
            this.connectThreadFactory = threadFactory;
            return this;
        }

        /**
         * Sets custom thread factory for the executor service
         *
         * @param threadFactory the thread factory to use for the executor service
         * @return the Builder for chaining
         */
        public Builder callbackThreadFactory(ThreadFactory threadFactory) {
            this.callbackThreadFactory = threadFactory;
            return this;
        }

        /**
         * Add an HttpRequest interceptor which can be used to modify the HTTP request when using websockets
         *
         * @param interceptor The interceptor
         * @return the Builder for chaining
         */
        public Builder httpRequestInterceptor(java.util.function.Consumer<HttpRequest> interceptor) {
            if (null == this.httpRequestInterceptors) {
                this.httpRequestInterceptors = new ArrayList<>();
            }
            this.httpRequestInterceptors.add(interceptor);
            return this;
        }

        /**
         * Overwrite the list of HttpRequest interceptors which can be used to modify the HTTP request when using websockets
         *
         * @param interceptors The list of interceptors
         * @return the Builder for chaining
         */
        public Builder httpRequestInterceptors(Collection<? extends java.util.function.Consumer<HttpRequest>> interceptors) {
            this.httpRequestInterceptors = new ArrayList<>(interceptors);
            return this;
        }

        /**
         * Define a proxy to use when connecting.
         *
         * @param proxy is the HTTP or socks proxy to use.
         * @return the Builder for chaining
         */
        public Builder proxy(Proxy proxy) {
            this.proxy = proxy;
            return this;
        }

        /**
         * The class to use for this connections data port. This is an advanced setting
         * and primarily useful for testing.
         *
         * @param dataPortClassName a valid and accessible class name
         * @return the Builder for chaining
         */
        public Builder dataPortType(String dataPortClassName) {
            this.dataPortType = dataPortClassName == null ? DEFAULT_DATA_PORT_TYPE : dataPortClassName;
            return this;
        }

        /**
         * Set the maximum number of messages in the outgoing queue.
         *
         * @param maxMessagesInOutgoingQueue the maximum number of messages in the outgoing queue
         * @return the Builder for chaining
         */
        public Builder maxMessagesInOutgoingQueue(int maxMessagesInOutgoingQueue) {
            this.maxMessagesInOutgoingQueue = maxMessagesInOutgoingQueue < 0
                ? DEFAULT_MAX_MESSAGES_IN_OUTGOING_QUEUE
                : maxMessagesInOutgoingQueue;
            return this;
        }

        /**
         * Enable discard messages when the outgoing queue full. See {@link Builder#maxMessagesInOutgoingQueue(int) maxMessagesInOutgoingQueue}
         *
         * @return the Builder for chaining
         */
        public Builder discardMessagesWhenOutgoingQueueFull() {
            this.discardMessagesWhenOutgoingQueueFull = true;
            return this;
        }

        /**
         * Turn off use of discovered servers when connecting / reconnecting. Used in the default server list provider.
         * @return the Builder for chaining
         */
        public Builder ignoreDiscoveredServers() {
            this.ignoreDiscoveredServers = true;
            return this;
        }

        /**
         * Set TLS Handshake First behavior on. Default is off.
         * TLS Handshake First is used to instruct the library perform
         * the TLS handshake right after the connect and before receiving
         * the INFO protocol from the server. If this option is enabled
         * but the server is not configured to perform the TLS handshake
         * first, the connection will fail.
         * @return the Builder for chaining
         */
        public Builder tlsFirst() {
            this.tlsFirst = true;
            return this;
        }

        /**
         * Throw {@link java.util.concurrent.TimeoutException} on timeout instead of {@link java.util.concurrent.CancellationException}?
         * @return the Builder for chaining
         */
        public Builder useTimeoutException() {
            this.useTimeoutException = true;
            return this;
        }

        /**
         * Instruct dispatchers to dispatch all messages as a task, instead of directly from dispatcher thread
         * @return the Builder for chaining
         */
        public Builder useDispatcherWithExecutor() {
            this.useDispatcherWithExecutor = true;
            return this;
        }

        /**
         * Instruct requests to turn off flush on requests.
         * @return the Builder for chaining
         */
        public Builder dontForceFlushOnRequest() {
            this.forceFlushOnRequest = false;
            return this;
        }

        /**
         * Set the ServerPool implementation for connections to use instead of the default implementation
         * @param serverPool the implementation
         * @return the Builder for chaining
         */
        public Builder serverPool(ServerPool serverPool) {
            this.serverPool = serverPool;
            return this;
        }

        /**
         * Set the DispatcherFactory implementation for connections to use instead of the default implementation
         * @param dispatcherFactory the implementation
         * @return the Builder for chaining
         */
        public Builder dispatcherFactory(DispatcherFactory dispatcherFactory) {
            this.dispatcherFactory = dispatcherFactory;
            return this;
        }

        /**
         * Whether to enable Fast fallback algorithm for socket connect
         * @return the Builder for chaining
         */
        public Builder enableFastFallback() {
            this.enableFastFallback = true;
            return this;
        }

        /**
         * Build an Options object from this Builder.
         *
         * <p>If the Options builder was not provided with a server, a default one will be included
         * {@link Options#DEFAULT_URL}. If only a single server URI is included, the builder
         * will try a few things to make connecting easier:
         * <ul>
         * <li>If there is no user/password is set but the URI has them, {@code nats://user:password@server:port}, they will be used.
         * <li>If there is no token is set but the URI has one, {@code nats://token@server:port}, it will be used.
         * <li>If the URI is of the form tls:// and no SSL context was assigned, one is created, see {@link Options.Builder#secure() secure()}.
         * <li>If the URI is of the form opentls:// and no SSL context was assigned one will be created
         * that does not check the servers certificate for validity. This is not secure and only provided
         * for tests and development.
         * </ul>
         *
         * @return the new options object
         * @throws IllegalStateException if there is a conflict in the options, like a token and a user/pass
         */
        public Options build() throws IllegalStateException {
            // ----------------------------------------------------------------------------------------------------
            // BUILD IMPL
            // ----------------------------------------------------------------------------------------------------
            if (this.username != null && tokenSupplier.get() != null) {
                throw new IllegalStateException("Options can't have token and username");
            }

            if (inboxPrefix == null) {
                inboxPrefix = DEFAULT_INBOX_PREFIX;
            }

            boolean checkUrisForSecure = true;
            if (natsServerUris.isEmpty()) {
                server(DEFAULT_URL);
                checkUrisForSecure = false;
            }

            // ssl context can be directly provided, but if it's not
            // there might be a factory, or just see if we should make it ourselves
            if (sslContext == null) {
                if (sslContextFactory != null) {
                    sslContext = sslContextFactory.createSSLContext(new SSLContextFactoryProperties.Builder()
                        .keystore(keystore)
                        .keystorePassword(keystorePassword)
                        .truststore(truststore)
                        .truststorePassword(truststorePassword)
                        .tlsAlgorithm(tlsAlgorithm)
                        .build());
                }
                else {
                    if (keystore != null || truststore != null) {
                        // the user provided keystore/truststore properties, the want us to make the sslContext that way
                        try {
                            sslContext = SSLUtils.createSSLContext(keystore, keystorePassword, truststore, truststorePassword, tlsAlgorithm);
                        }
                        catch (Exception e) {
                            throw new IllegalStateException("Unable to create SSL context", e);
                        }
                    }
                    else {
                        // the sslContext has not been requested via factory or keystore/truststore properties
                        // If we haven't been told to use the default or the trust all context
                        // and the server isn't the default url, check to see if the server uris
                        // suggest we need the ssl context.
                        if (!useDefaultTls && !useTrustAllTls && checkUrisForSecure) {
                            for (int i = 0; sslContext == null && i < natsServerUris.size(); i++) {
                                NatsUri natsUri = natsServerUris.get(i);
                                switch (natsUri.getScheme()) {
                                    case TLS_PROTOCOL:
                                    case SECURE_WEBSOCKET_PROTOCOL:
                                        useDefaultTls = true;
                                        break;
                                    case OPENTLS_PROTOCOL:
                                        useTrustAllTls = true;
                                        break;
                                }
                            }
                        }

                        // check trust all (open) first, in case they provided both
                        // PROP_SECURE (secure) and PROP_OPENTLS (opentls)
                        if (useTrustAllTls) {
                            try {
                                this.sslContext = SSLUtils.createTrustAllTlsContext();
                            }
                            catch (GeneralSecurityException e) {
                                throw new IllegalStateException("Unable to create SSL context", e);
                            }
                        }
                        else if (useDefaultTls) {
                            try {
                                this.sslContext = SSLContext.getDefault();
                            }
                            catch (NoSuchAlgorithmException e) {
                                throw new IllegalStateException("Unable to create default SSL context", e);
                            }
                        }
                    }
                }
            }

            if (tlsFirst && sslContext == null) {
                throw new IllegalStateException("SSL context required for tls handshake first");
            }

            if (credentialPath != null) {
                File file = new File(credentialPath).getAbsoluteFile();
                authHandler = Nats.credentials(file.toString());
            }

            if (socketReadTimeoutMillis > 0) {
                long srtMin = pingInterval.toMillis() + MINIMUM_SOCKET_WRITE_TIMEOUT_GT_CONNECTION_TIMEOUT;
                if (socketReadTimeoutMillis < srtMin) {
                    throw new IllegalStateException("Socket Read Timeout must be at least "
                        + MINIMUM_SOCKET_READ_TIMEOUT_GT_CONNECTION_TIMEOUT
                        + " milliseconds greater than the Ping Interval");
                }
            }

            if (socketWriteTimeout == null || socketWriteTimeout.toMillis() < 1) {
                socketWriteTimeout = null;
            }
            else {
                long swtMin = connectionTimeout.toMillis() + MINIMUM_SOCKET_WRITE_TIMEOUT_GT_CONNECTION_TIMEOUT;
                if (socketWriteTimeout.toMillis() < swtMin) {
                    throw new IllegalStateException("Socket Write Timeout must be at least "
                        + MINIMUM_SOCKET_WRITE_TIMEOUT_GT_CONNECTION_TIMEOUT
                        + " milliseconds greater than the Connection Timeout");
                }
            }

            if (socketSoLinger < 0) {
                socketSoLinger = -1;
            }

            if (errorListener == null) {
                errorListener = new ErrorListenerLoggerImpl();
            }

            if (timeTraceLogger == null) {
                if (traceConnection) {
                    timeTraceLogger = (format, args) -> {
                        String timeStr = DateTimeFormatter.ISO_TIME.format(LocalDateTime.now());
                        System.out.println("[" + timeStr + "] connect trace: " + String.format(format, args));
                    };
                }
                else {
                    timeTraceLogger = (f, a) -> {};
                }
            }
            else {
                // if the dev provided an impl, we assume they meant to time trace the connection
                traceConnection = true;
            }

            return new Options(this);
        }

        // ----------------------------------------------------------------------------------------------------
        // BUILDER COPY CONSTRUCTOR
        // ----------------------------------------------------------------------------------------------------
        public Builder(Options o) {
            if (o == null) {
                throw new IllegalArgumentException("Options cannot be null");
            }

            this.natsServerUris.addAll(o.natsServerUris);
            this.unprocessedServers.addAll(o.unprocessedServers);
            this.noRandomize = o.noRandomize;
            this.noResolveHostnames = o.noResolveHostnames;
            this.reportNoResponders = o.reportNoResponders;
            this.connectionName = o.connectionName;
            this.verbose = o.verbose;
            this.pedantic = o.pedantic;
            this.sslContext = o.sslContext;
            this.maxReconnect = o.maxReconnect;
            this.reconnectWait = o.reconnectWait;
            this.reconnectJitter = o.reconnectJitter;
            this.reconnectJitterTls = o.reconnectJitterTls;
            this.connectionTimeout = o.connectionTimeout;
            this.socketReadTimeoutMillis = o.socketReadTimeoutMillis;
            this.socketWriteTimeout = o.socketWriteTimeout;
            this.socketSoLinger = o.socketSoLinger;
            this.pingInterval = o.pingInterval;
            this.requestCleanupInterval = o.requestCleanupInterval;
            this.maxPingsOut = o.maxPingsOut;
            this.reconnectBufferSize = o.reconnectBufferSize;
            this.username = o.username;
            this.password = o.password;
            this.tokenSupplier = o.tokenSupplier;
            this.useOldRequestStyle = o.useOldRequestStyle;
            this.maxControlLine = o.maxControlLine;
            this.bufferSize = o.bufferSize;
            this.noEcho = o.noEcho;
            this.noHeaders = o.noHeaders;
            this.noNoResponders = o.noNoResponders;
            this.clientSideLimitChecks = o.clientSideLimitChecks;
            this.supportUTF8Subjects = o.supportUTF8Subjects;
            this.inboxPrefix = o.inboxPrefix;
            this.traceConnection = o.traceConnection;
            this.maxMessagesInOutgoingQueue = o.maxMessagesInOutgoingQueue;
            this.discardMessagesWhenOutgoingQueueFull = o.discardMessagesWhenOutgoingQueueFull;

            this.authHandler = o.authHandler;
            this.reconnectDelayHandler = o.reconnectDelayHandler;

            this.errorListener = o.errorListener;
            this.timeTraceLogger = o.timeTraceLogger;
            this.connectionListener = o.connectionListener;
            this.readListener = o.readListener;
            this.statisticsCollector = o.statisticsCollector;
            this.dataPortType = o.dataPortType;
            this.trackAdvancedStats = o.trackAdvancedStats;
            this.executor = o.executor;
            this.scheduledExecutor = o.scheduledExecutor;
            this.callbackThreadFactory = o.callbackThreadFactory;
            this.connectThreadFactory = o.connectThreadFactory;
            this.httpRequestInterceptors = o.httpRequestInterceptors;
            this.proxy = o.proxy;

            this.ignoreDiscoveredServers = o.ignoreDiscoveredServers;
            this.tlsFirst = o.tlsFirst;
            this.useTimeoutException = o.useTimeoutException;
            this.useDispatcherWithExecutor = o.useDispatcherWithExecutor;
            this.forceFlushOnRequest = o.forceFlushOnRequest;

            this.serverPool = o.serverPool;
            this.dispatcherFactory = o.dispatcherFactory;
            this.enableFastFallback = o.enableFastFallback;
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // CONSTRUCTOR
    // ----------------------------------------------------------------------------------------------------
    private Options(Builder b) {
        this.natsServerUris = Collections.unmodifiableList(b.natsServerUris);
        this.unprocessedServers = Collections.unmodifiableList(b.unprocessedServers);  // exactly how the user gave them
        this.noRandomize = b.noRandomize;
        this.noResolveHostnames = b.noResolveHostnames;
        this.reportNoResponders = b.reportNoResponders;
        this.connectionName = b.connectionName;
        this.verbose = b.verbose;
        this.pedantic = b.pedantic;
        this.sslContext = b.sslContext;
        this.maxReconnect = b.maxReconnect;
        this.reconnectWait = b.reconnectWait;
        this.reconnectJitter = b.reconnectJitter;
        this.reconnectJitterTls = b.reconnectJitterTls;
        this.connectionTimeout = b.connectionTimeout;
        this.socketReadTimeoutMillis = b.socketReadTimeoutMillis;
        this.socketWriteTimeout = b.socketWriteTimeout;
        this.socketSoLinger = b.socketSoLinger;
        this.pingInterval = b.pingInterval;
        this.requestCleanupInterval = b.requestCleanupInterval;
        this.maxPingsOut = b.maxPingsOut;
        this.reconnectBufferSize = b.reconnectBufferSize;
        this.username = b.username;
        this.password = b.password;
        this.tokenSupplier = b.tokenSupplier;
        this.useOldRequestStyle = b.useOldRequestStyle;
        this.maxControlLine = b.maxControlLine;
        this.bufferSize = b.bufferSize;
        this.noEcho = b.noEcho;
        this.noHeaders = b.noHeaders;
        this.noNoResponders = b.noNoResponders;
        this.clientSideLimitChecks = b.clientSideLimitChecks;
        this.supportUTF8Subjects = b.supportUTF8Subjects;
        this.inboxPrefix = b.inboxPrefix;
        this.traceConnection = b.traceConnection;
        this.maxMessagesInOutgoingQueue = b.maxMessagesInOutgoingQueue;
        this.discardMessagesWhenOutgoingQueueFull = b.discardMessagesWhenOutgoingQueueFull;

        this.authHandler = b.authHandler;
        this.reconnectDelayHandler = b.reconnectDelayHandler;

        this.errorListener = b.errorListener;
        this.timeTraceLogger = b.timeTraceLogger;
        this.connectionListener = b.connectionListener;
        this.readListener = b.readListener;
        this.statisticsCollector = b.statisticsCollector;
        this.dataPortType = b.dataPortType;
        this.trackAdvancedStats = b.trackAdvancedStats;
        this.executor = b.executor;
        this.scheduledExecutor = b.scheduledExecutor;
        this.callbackThreadFactory = b.callbackThreadFactory;
        this.connectThreadFactory = b.connectThreadFactory;
        this.httpRequestInterceptors = b.httpRequestInterceptors;
        this.proxy = b.proxy;

        this.ignoreDiscoveredServers = b.ignoreDiscoveredServers;
        this.tlsFirst = b.tlsFirst;
        this.useTimeoutException = b.useTimeoutException;
        this.useDispatcherWithExecutor = b.useDispatcherWithExecutor;
        this.forceFlushOnRequest = b.forceFlushOnRequest;

        this.serverPool = b.serverPool;
        this.dispatcherFactory = b.dispatcherFactory;
        this.enableFastFallback = b.enableFastFallback;
    }

    // ----------------------------------------------------------------------------------------------------
    // GETTERS
    // ----------------------------------------------------------------------------------------------------
    /**
     * @return the executor, see {@link Builder#executor(ExecutorService) executor()} in the builder doc
     */
    public ExecutorService getExecutor() {
        return this.executor == null ? _getInternalExecutor() : this.executor;
    }

    private ExecutorService _getInternalExecutor() {
        String threadPrefix = nullOrEmpty(this.connectionName) ? DEFAULT_THREAD_NAME_PREFIX : this.connectionName;
        return new ThreadPoolExecutor(0, Integer.MAX_VALUE,
            500L, TimeUnit.MILLISECONDS,
            new SynchronousQueue<>(),
            new DefaultThreadFactory(threadPrefix));
    }

    /**
     * @return the ScheduledExecutorService, see {@link Builder#scheduledExecutor(ScheduledExecutorService) scheduledExecutor()} in the builder doc
     */
    public ScheduledExecutorService getScheduledExecutor() {
        return this.scheduledExecutor == null ? _getInternalScheduledExecutor() : this.scheduledExecutor;
    }

    private ScheduledExecutorService _getInternalScheduledExecutor() {
        String threadPrefix = nullOrEmpty(this.connectionName) ? DEFAULT_THREAD_NAME_PREFIX : this.connectionName;
        // the core pool size of 3 is chosen considering where we know the scheduler is used.
        // 1. Ping timer, 2. cleanup timer, 3. SocketDataPortWithWriteTimeout
        // Pull message managers also use a scheduler, but we don't even know if this will be consuming
        ScheduledThreadPoolExecutor stpe = new ScheduledThreadPoolExecutor(3, new DefaultThreadFactory(threadPrefix));
        stpe.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        stpe.setRemoveOnCancelPolicy(true);
        return stpe;
    }

    public boolean executorIsInternal() {
        return this.executor == null;
    }

    public boolean scheduledExecutorIsInternal() {
        return this.scheduledExecutor == null;
    }

    /**
     * @return the callback executor, see {@link Builder#callbackThreadFactory(ThreadFactory) callbackThreadFactory()} in the builder doc
     */
    public ExecutorService getCallbackExecutor() {
        return this.callbackThreadFactory == null ?
                DEFAULT_SINGLE_THREAD_EXECUTOR.get() : Executors.newSingleThreadExecutor(this.callbackThreadFactory);
    }

    /**
     * @return the connect executor, see {@link Builder#connectThreadFactory(ThreadFactory) connectThreadFactory()} in the builder doc
     */
    public ExecutorService getConnectExecutor() {
        return this.connectThreadFactory == null ?
                DEFAULT_SINGLE_THREAD_EXECUTOR.get() : Executors.newSingleThreadExecutor(this.connectThreadFactory);
    }

    /**
     * @return the list of HttpRequest interceptors.
     */
    public List<java.util.function.Consumer<HttpRequest>> getHttpRequestInterceptors() {
        return null == this.httpRequestInterceptors
            ? Collections.emptyList()
            : Collections.unmodifiableList(this.httpRequestInterceptors);
    }

    /**
     * @return the proxy to used for all sockets.
     */
    public Proxy getProxy() {
        return this.proxy;
    }

    /**
     * @return the error listener. Will be an instance of ErrorListenerLoggerImpl if not user supplied. See {@link Builder#errorListener(ErrorListener) errorListener()} in the builder doc
     */
    public ErrorListener getErrorListener() {
        return this.errorListener;
    }

    /**
     * If the user provided a TimeTraceLogger, it's returned here.
     * If the user set traceConnection but did not supply their own, the original time trace logging will occur
     * If the user did not provide a TimeTraceLogger and did not set traceConnection, this will be a no-op implementation.
     * @return the time trace logger
     */
    public TimeTraceLogger getTimeTraceLogger() {
        return this.timeTraceLogger;
    }

    /**
     * @return the connection listener, or null, see {@link Builder#connectionListener(ConnectionListener) connectionListener()} in the builder doc
     */
    public ConnectionListener getConnectionListener() {
        return this.connectionListener;
    }

    /**
     * @return the read listener, or null, see {@link Builder#readListener(ReadListener) readListener()} in the builder doc
     */
    public ReadListener getReadListener() {
        return this.readListener;
    }

    /**
     * @return the statistics collector, or null, see {@link Builder#statisticsCollector(StatisticsCollector) statisticsCollector()} in the builder doc
     */
    public StatisticsCollector getStatisticsCollector() {
        return this.statisticsCollector;
    }

    /**
     * @return the auth handler, or null, see {@link Builder#authHandler(AuthHandler) authHandler()} in the builder doc
     */
    public AuthHandler getAuthHandler() {
        return this.authHandler;
    }

    /**
     * @return the reconnection delay handler, or null, see {@link Builder#reconnectDelayHandler(ReconnectDelayHandler) reconnectDelayHandler()} in the builder doc
     */
    public ReconnectDelayHandler getReconnectDelayHandler() {
        return this.reconnectDelayHandler;
    }

    /**
     * @return the dataport type for connections created by this options object, see {@link Builder#dataPortType(String) dataPortType()} in the builder doc
     */
    public String getDataPortType() {
        return this.dataPortType;
    }

    /**
     * @return the data port described by these options
     */
    public DataPort buildDataPort() {
        DataPort dp;
        if (dataPortType.equals(DEFAULT_DATA_PORT_TYPE)) {
            if (socketWriteTimeout == null) {
                dp = new SocketDataPort();
            }
            else {
                dp = new SocketDataPortWithWriteTimeout();
            }
        }
        else {
            dp = (DataPort) Options.createInstanceOf(dataPortType);
        }
        dp.afterConstruct(this);
        return dp;
    }

    /**
     * @return the servers configured in options, see {@link Builder#servers(String[]) servers()} in the builder doc
     */
    public List<URI> getServers() {
        List<URI> list = new ArrayList<>();
        for (NatsUri nuri : natsServerUris) {
            list.add(nuri.getUri());
        }
        return list;
    }

    /**
     * @return the servers configured in options, see {@link Builder#servers(String[]) servers()} in the builder doc
     */
    public List<NatsUri> getNatsServerUris() {
        return natsServerUris;
    }

    /**
     * @return the servers as given to the options, since the servers are normalized
     */
    public List<String> getUnprocessedServers() {
        return unprocessedServers;
    }

    /**
     * @return should we turn off randomization for server connection attempts, see {@link Builder#noRandomize() noRandomize()} in the builder doc
     */
    public boolean isNoRandomize() {
        return noRandomize;
    }

    /**
     * @return should we resolve hostnames for server connection attempts, see {@link Builder#noResolveHostnames() noResolveHostnames()} in the builder doc
     */
    public boolean isNoResolveHostnames() {
        return noResolveHostnames;
    }

    /**
     * @return should complete with exception futures for requests that get no responders instead of cancelling the future, see {@link Builder#reportNoResponders() reportNoResponders()} in the builder doc
     */
    public boolean isReportNoResponders() {
        return reportNoResponders;
    }

    /**
     * @return the connectionName, see {@link Builder#connectionName(String) connectionName()} in the builder doc
     */
    public String getConnectionName() {
        return connectionName;
    }

    /**
     * @return are we in verbose mode, see {@link Builder#verbose() verbose()} in the builder doc
     */
    public boolean isVerbose() {
        return verbose;
    }

    /**
     * @return is echo-ing disabled, see {@link Builder#noEcho() noEcho()} in the builder doc
     */
    public boolean isNoEcho() {
        return noEcho;
    }

    /**
     * @return are headers disabled, see {@link Builder#noHeaders() noHeaders()} in the builder doc
     */
    public boolean isNoHeaders() {
        return noHeaders;
    }

    /**
     * @return is NoResponders ignored disabled, see {@link Builder#noNoResponders() noNoResponders()} in the builder doc
     */
    public boolean isNoNoResponders() {
        return noNoResponders;
    }

    /**
     * @return clientSideLimitChecks flag
     */
    public boolean clientSideLimitChecks() {
        return clientSideLimitChecks;
    }

    /**
     * @return whether utf8 subjects are supported, see {@link Builder#supportUTF8Subjects() supportUTF8Subjects()} in the builder doc.
     */
    public boolean supportUTF8Subjects() {
        return supportUTF8Subjects;
    }

    /**
     * @return are we using pedantic protocol, see {@link Builder#pedantic() pedantic()} in the builder doc
     */
    public boolean isPedantic() {
        return pedantic;
    }

    /**
     * @return should we track advanced stats, see {@link Builder#turnOnAdvancedStats() turnOnAdvancedStats()} in the builder doc
     */
    public boolean isTrackAdvancedStats() {
        return trackAdvancedStats;
    }

    /**
     * If isTraceConnection is true, the user provided a TimeTraceLogger or manually called traceConnection in the builder
     * @return should we trace the connection?
     */
    public boolean isTraceConnection() {
        return traceConnection;
    }

    /**
     * @return the maximum length of a control line, see {@link Builder#maxControlLine(int) maxControlLine()} in the builder doc
     */
    public int getMaxControlLine() {
        return maxControlLine;
    }

    /**
     *
     * @return true if there is an sslContext for these Options, otherwise false, see {@link Builder#secure() secure()} in the builder doc
     */
    public boolean isTLSRequired() {
        return sslContext != null;
    }

    /**
     * @return the sslContext, see {@link Builder#secure() secure()} in the builder doc
     */
    public SSLContext getSslContext() {
        return sslContext;
    }

    /**
     * @return the maxReconnect attempts to make before failing, see {@link Builder#maxReconnects(int) maxReconnects()} in the builder doc
     */
    public int getMaxReconnect() {
        return maxReconnect;
    }

    /**
     * @return the reconnectWait, used between reconnect attempts, see {@link Builder#reconnectWait(Duration) reconnectWait()} in the builder doc
     */
    public Duration getReconnectWait() {
        return reconnectWait;
    }

    /**
     * @return the reconnectJitter, used between reconnect attempts to vary the reconnect wait, see {@link Builder#reconnectJitter(Duration) reconnectJitter()} in the builder doc
     */
    public Duration getReconnectJitter() {
        return reconnectJitter;
    }

    /**
     * @return the reconnectJitterTls, used between reconnect attempts to vary the reconnect wait whe using tls/secure, see {@link Builder#reconnectJitterTls(Duration) reconnectJitterTls()} in the builder doc
     */
    public Duration getReconnectJitterTls() {
        return reconnectJitterTls;
    }

    /**
     * @return the connectionTimeout, see {@link Builder#connectionTimeout(Duration) connectionTimeout()} in the builder doc
     */
    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * @return the socketReadTimeoutMillis, see {@link Builder#socketReadTimeoutMillis(int) socketReadTimeoutMillis} in the builder doc
     */
    public int getSocketReadTimeoutMillis() {
        return socketReadTimeoutMillis;
    }

    /**
     * @return the socketWriteTimeout, see {@link Builder#socketWriteTimeout(long) socketWriteTimeout} in the builder doc
     */
    public Duration getSocketWriteTimeout() {
        return socketWriteTimeout;
    }

    /**
     * @return the socket so linger number of seconds, see {@link Builder#socketSoLinger(int) socketSoLinger()} in the builder doc
     */
    public int getSocketSoLinger() {
        return socketSoLinger;
    }

    /**
     * @return the pingInterval, see {@link Builder#pingInterval(Duration) pingInterval()} in the builder doc
     */
    public Duration getPingInterval() {
        return pingInterval;
    }

    /**
     * @return the request cleanup interval, see {@link Builder#requestCleanupInterval(Duration) requestCleanupInterval()} in the builder doc
     */
    public Duration getRequestCleanupInterval() {
        return requestCleanupInterval;
    }

    /**
     * @return the maxPingsOut to limit the number of pings on the wire, see {@link Builder#maxPingsOut(int) maxPingsOut()} in the builder doc
     */
    public int getMaxPingsOut() {
        return maxPingsOut;
    }

    /**
     * @return the reconnectBufferSize, to limit the amount of data held during
     *         reconnection attempts, see {@link Builder#reconnectBufferSize(long) reconnectBufferSize()} in the builder doc
     */
    public long getReconnectBufferSize() {
        return reconnectBufferSize;
    }

    /**
     * @return the default size for buffers in the connection code, see {@link Builder#bufferSize(int) bufferSize()} in the builder doc
     */
    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * @deprecated converts the char array to a string, use getUserNameChars instead for more security
     * @return the username to use for basic authentication, see {@link Builder#userInfo(String, String) userInfo()} in the builder doc
     */
    @Deprecated
    public String getUsername() {
        return username == null ? null : new String(username);
    }

    /**
     * @return the username to use for basic authentication, see {@link Builder#userInfo(String, String) userInfo()} in the builder doc
     */
    public char[] getUsernameChars() {
        return username;
    }

    /**
     * @deprecated converts the char array to a string, use getPasswordChars instead for more security
     * @return the password to use for basic authentication, see {@link Builder#userInfo(String, String) userInfo()} in the builder doc
     */
    @Deprecated
    public String getPassword() {
        return password == null ? null : new String(password);
    }

    /**
     * @return the password to use for basic authentication, see {@link Builder#userInfo(String, String) userInfo()} in the builder doc
     */
    public char[] getPasswordChars() {
        return password;
    }

    /**
     * @deprecated converts the char array to a string, use getTokenChars instead for more security
     * @return the token to be used for token-based authentication, see {@link Builder#token(String) token()} in the builder doc
     */
    @Deprecated
    public String getToken() {
        char[] token = tokenSupplier.get();
        return token == null ? null : new String(token);
    }

    /**
     * @return the token to be used for token-based authentication, see {@link Builder#token(String) token()} in the builder doc
     */
    public char[] getTokenChars() {
        return tokenSupplier.get();
    }

    /**
     * @return the flag to turn on old style requests, see {@link Builder#oldRequestStyle() oldStyleRequest()} in the builder doc
     */
    public boolean isOldRequestStyle() {
        return useOldRequestStyle;
    }

    /**
     * @return the inbox prefix to use for requests, see {@link Builder#inboxPrefix(String) inboxPrefix()} in the builder doc
     */
    public String getInboxPrefix() {
        return inboxPrefix;
    }

    /**
     * @return the maximum number of messages in the outgoing queue, see {@link Builder#maxMessagesInOutgoingQueue(int)
     * maxMessagesInOutgoingQueue(int)} in the builder doc
     */
    public int getMaxMessagesInOutgoingQueue() {
        return maxMessagesInOutgoingQueue;
    }

    /**
     * @return should we discard messages when the outgoing queue is full, see {@link Builder#discardMessagesWhenOutgoingQueueFull()
     * discardMessagesWhenOutgoingQueueFull()} in the builder doc
     */
    public boolean isDiscardMessagesWhenOutgoingQueueFull() {
        return discardMessagesWhenOutgoingQueueFull;
    }

    /**
     * Get whether to ignore discovered servers
     * @return the flag
     */
    public boolean isIgnoreDiscoveredServers() {
        return ignoreDiscoveredServers;
    }

    /**
     * Get whether to do tls first
     * @return the flag
     */
    public boolean isTlsFirst() {
        return tlsFirst;
    }

    /**
     * Get whether to throw {@link java.util.concurrent.TimeoutException} on timeout instead of {@link java.util.concurrent.CancellationException}.
     * @return the flag
     */
    public boolean useTimeoutException() {
        return useTimeoutException;
    }

    /**
     * Whether the dispatcher should use an executor to async messages to handlers
     * @return the flag
     */
    public boolean useDispatcherWithExecutor() { return useDispatcherWithExecutor; }

    /**
     * Whether to flush on any user request
     * @return the flag
     */
    public boolean forceFlushOnRequest() {
        return forceFlushOnRequest;
    }

    /**
     * Get the ServerPool implementation. If null, a default implementation is used.
     * @return the ServerPool implementation
     */
    public ServerPool getServerPool() {
        return serverPool;
    }

    /**
     * Get the DispatcherFactory implementation. If null, a default implementation is used.
     * @return the DispatcherFactory implementation
     */
    public DispatcherFactory getDispatcherFactory() {
        return dispatcherFactory;
    }

    /**
     * Whether Fast fallback algorithm is enabled for socket connect
     * @return the flag
     */
    public boolean isEnableFastFallback() {
        return enableFastFallback;
    }

    public URI createURIForServer(String serverURI) throws URISyntaxException {
        return new NatsUri(serverURI).getUri();
    }

    /**
     * Create the options string sent with the connect message.
     * If includeAuth is true the auth information is included:
     * If the server URIs have auth info it is used. Otherwise, the userInfo is used.
     * @param serverURI the current server uri
     * @param includeAuth tells the options to build a connection string that includes auth information
     * @param nonce if the client is supposed to sign the nonce for authentication
     * @return the options String, basically JSON
     */
    public CharBuffer buildProtocolConnectOptionsString(String serverURI, boolean includeAuth, byte[] nonce) {
        CharBuffer connectString = CharBuffer.allocate(this.maxControlLine);
        connectString.append("{");

        appendOption(connectString, Options.OPTION_LANG, Nats.CLIENT_LANGUAGE, true, false);
        appendOption(connectString, Options.OPTION_VERSION, Nats.CLIENT_VERSION, true, true);

        if (this.connectionName != null) {
            appendOption(connectString, Options.OPTION_NAME, this.connectionName, true, true);
        }

        appendOption(connectString, Options.OPTION_PROTOCOL, "1", false, true);

        appendOption(connectString, Options.OPTION_VERBOSE, String.valueOf(this.isVerbose()), false, true);
        appendOption(connectString, Options.OPTION_PEDANTIC, String.valueOf(this.isPedantic()), false, true);
        appendOption(connectString, Options.OPTION_TLS_REQUIRED, String.valueOf(this.isTLSRequired()), false, true);
        appendOption(connectString, Options.OPTION_ECHO, String.valueOf(!this.isNoEcho()), false, true);
        appendOption(connectString, Options.OPTION_HEADERS, String.valueOf(!this.isNoHeaders()), false, true);
        appendOption(connectString, Options.OPTION_NORESPONDERS, String.valueOf(!this.isNoNoResponders()), false, true);

        if (includeAuth) {
            if (nonce != null && this.getAuthHandler() != null) {
                char[] nkey = this.getAuthHandler().getID();
                byte[] sig = this.getAuthHandler().sign(nonce);
                char[] jwt = this.getAuthHandler().getJWT();

                if (sig == null) {
                    sig = new byte[0];
                }

                if (jwt == null) {
                    jwt = new char[0];
                }

                if (nkey == null) {
                    nkey = new char[0];
                }

                String encodedSig = base64UrlEncodeToString(sig);

                appendOption(connectString, Options.OPTION_NKEY, nkey, true);
                appendOption(connectString, Options.OPTION_SIG, encodedSig, true, true);
                appendOption(connectString, Options.OPTION_JWT, jwt, true);
            }

            String uriUser = null;
            String uriPass = null;
            String uriToken = null;

            // Values from URI override options
            try {
                URI uri = this.createURIForServer(serverURI);
                String userInfo = uri.getRawUserInfo();
                if (userInfo != null) {
                    int at = userInfo.indexOf(":");
                    if (at == -1) {
                        uriToken = uriDecode(userInfo);
                    }
                    else {
                        uriUser = uriDecode(userInfo.substring(0, at));
                        uriPass = uriDecode(userInfo.substring(at + 1));
                    }
                }
            }
            catch (URISyntaxException e) {
                // the createURIForServer call is the one that potentially throws this
                // uriUser, uriPass and uriToken will already be null
            }

            if (uriUser != null) {
                appendOption(connectString, Options.OPTION_USER, jsonEncode(uriUser), true, true);
            }
            else if (this.username != null) {
                appendOption(connectString, Options.OPTION_USER, jsonEncode(this.username), true, true);
            }

            if (uriPass != null) {
                appendOption(connectString, Options.OPTION_PASSWORD, jsonEncode(uriPass), true, true);
            }
            else if (this.password != null) {
                appendOption(connectString, Options.OPTION_PASSWORD, jsonEncode(this.password), true, true);
            }

            if (uriToken != null) {
                appendOption(connectString, Options.OPTION_AUTH_TOKEN, uriToken, true, true);
            }
            else {
                char[] token = this.tokenSupplier.get();
                if (token != null) {
                    appendOption(connectString, Options.OPTION_AUTH_TOKEN, token, true);
                }
            }
        }

        connectString.append("}");
        connectString.flip();
        return connectString;
    }

    // ----------------------------------------------------------------------------------------------------
    // HELPER FUNCTIONS
    // ----------------------------------------------------------------------------------------------------
    private static void appendOption(CharBuffer builder, String key, String value, boolean quotes, boolean comma) {
        _appendStart(builder, key, quotes, comma);
        builder.append(value);
        _appendOptionEnd(builder, quotes);
    }

    @SuppressWarnings("SameParameterValue")
    private static void appendOption(CharBuffer builder, String key, char[] value, boolean comma) {
        _appendStart(builder, key, true, comma);
        builder.put(value);
        _appendOptionEnd(builder, true);
    }

    private static void _appendStart(CharBuffer builder, String key, boolean quotes, boolean comma) {
        if (comma) {
            builder.append(',');
        }
        builder.append('"');
        builder.append(key);
        builder.append('"');
        builder.append(':');
        _appendOptionEnd(builder, quotes);
    }

    private static void _appendOptionEnd(CharBuffer builder, boolean quotes) {
        if (quotes) {
            builder.append('"');
        }
    }

    private static String getPropertyValue(Properties props, String key) {
        String value = emptyAsNull(props.getProperty(key));
        if (value != null) {
            return value;
        }
        if (key.startsWith(PFX)) { // if the key starts with the PFX, check the non PFX
            return emptyAsNull(props.getProperty(key.substring(PFX_LEN)));
        }
        // otherwise check with the PFX
        value = emptyAsNull(props.getProperty(PFX + key));
        if (value == null && key.contains("_")) {
            // addressing where underscore was used in a key value instead of dot
            return getPropertyValue(props, key.replace("_", "."));
        }
        return value;
    }

    private static void stringProperty(Properties props, String key, java.util.function.Consumer<String> consumer) {
        String value = getPropertyValue(props, key);
        if (value != null) {
            consumer.accept(value);
        }
    }

    private static void charArrayProperty(Properties props, String key, java.util.function.Consumer<char[]> consumer) {
        String value = getPropertyValue(props, key);
        if (value != null) {
            consumer.accept(value.toCharArray());
        }
    }

    private static void booleanProperty(Properties props, String key, java.util.function.Consumer<Boolean> consumer) {
        String value = getPropertyValue(props, key);
        if (value != null) {
            consumer.accept(Boolean.parseBoolean(value));
        }
    }

    private static void intProperty(Properties props, String key, int defaultValue, java.util.function.Consumer<Integer> consumer) {
        String value = getPropertyValue(props, key);
        if (value == null) {
            consumer.accept(defaultValue);
        }
        else {
            consumer.accept(Integer.parseInt(value));
        }
    }

    private static void intGtEqZeroProperty(Properties props, String key, int defaultValue, java.util.function.Consumer<Integer> consumer) {
        String value = getPropertyValue(props, key);
        if (value == null) {
            consumer.accept(defaultValue);
        }
        else {
            int i = Integer.parseInt(value);
            if (i < 0) {
                consumer.accept(defaultValue);
            }
            else {
                consumer.accept(i);
            }
        }
    }

    private static void longProperty(Properties props, String key, long defaultValue, java.util.function.Consumer<Long> consumer) {
        String value = getPropertyValue(props, key);
        if (value == null) {
            consumer.accept(defaultValue);
        }
        else {
            consumer.accept(Long.parseLong(value));
        }
    }

    private static void durationProperty(Properties props, String key, Duration defaultValue, java.util.function.Consumer<Duration> consumer) {
        String value = getPropertyValue(props, key);
        if (value == null) {
            consumer.accept(defaultValue);
        }
        else {
            try {
                Duration d = Duration.parse(value);
                if (d.toNanos() < 0) {
                    consumer.accept(defaultValue);
                }
                else {
                    consumer.accept(d);
                }
            }
            catch (DateTimeParseException pe) {
                int ms = Integer.parseInt(value);
                if (ms < 0) {
                    consumer.accept(defaultValue);
                }
                else {
                    consumer.accept(Duration.ofMillis(ms));
                }
            }
        }
    }

    private static void classnameProperty(Properties props, String key, java.util.function.Consumer<Object> consumer) {
        stringProperty(props, key, className -> consumer.accept(createInstanceOf(className)));
    }

    private static Object createInstanceOf(String className) {
        try {
            Class<?> clazz = Class.forName(className);
            Constructor<?> constructor = clazz.getConstructor();
            return constructor.newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }
}
