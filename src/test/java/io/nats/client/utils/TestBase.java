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

package io.nats.client.utils;

import io.nats.client.*;
import io.nats.client.api.ServerInfo;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.*;
import io.nats.client.support.NatsJetStreamClientError;
import org.junit.jupiter.api.function.Executable;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import static io.nats.client.support.NatsConstants.DOT;
import static io.nats.client.support.NatsConstants.EMPTY;
import static io.nats.client.support.NatsJetStreamClientError.KIND_ILLEGAL_ARGUMENT;
import static io.nats.client.support.NatsJetStreamClientError.KIND_ILLEGAL_STATE;
import static org.junit.jupiter.api.Assertions.*;

public class TestBase {

    public static final String STAR_SEGMENT        = "*.star.*.segment.*";
    public static final String GT_NOT_LAST_SEGMENT = "gt.>.notlast";
    public static final String GT_LAST_SEGMENT     = "gt.last.>";
    public static final String STARTS_WITH_DOT     = ".starts-with-dot";
    public static final String ENDS_WITH_DOT       = "ends-with-dot.";
    public static final String ENDS_WITH_DOT_SPACE = "ends-with-space. ";
    public static final String ENDS_WITH_CR        = "ends-with-space.\r";
    public static final String ENDS_WITH_LF        = "ends-with-space.\n";
    public static final String ENDS_WITH_TAB       = "ends-with-space.\t";
    public static final String STAR_NOT_SEGMENT    = "star*not*segment";
    public static final String GT_NOT_SEGMENT      = "gt>not>segment";
    public static final String EMPTY_SEGMENT       = "blah..blah";

    public static final String PLAIN          = "plain";
    public static final String HAS_SPACE      = "has space";
    public static final String STARTS_SPACE   = " startsspace";
    public static final String ENDS_SPACE     = "endsspace ";
    public static final String HAS_PRINTABLE  = "has-print!able";
    public static final String HAS_DOT        = "has.dot";
    public static final String HAS_DASH       = "has-dash";
    public static final String HAS_UNDER      = "has_under";
    public static final String HAS_DOLLAR     = "has$dollar";
    public static final String HAS_CR         = "has\rcr";
    public static final String HAS_LF         = "has\nlf";
    public static final String HAS_TAB        = "has\ttab";
    public static final String HAS_LOW        = "has" + (char)0 + "low";
    public static final String HAS_127        = "has" + (char)127 + "127";
    public static final String HAS_FWD_SLASH  = "has/fwd/slash";
    public static final String HAS_BACK_SLASH = "has\\back\\slash";
    public static final String HAS_EQUALS     = "has=equals";
    public static final String HAS_TIC        = "has`tic";

    public static final String META_KEY   = "meta-test-key";
    public static final String META_VALUE = "meta-test-value";

    public static final long STANDARD_CONNECTION_WAIT_MS = 5000;
    public static final long LONG_CONNECTION_WAIT_MS = 7500;
    public static final long VERY_LONG_CONNECTION_WAIT_MS = 10000;
    public static final long STANDARD_FLUSH_TIMEOUT_MS = 2000;
    public static final long MEDIUM_FLUSH_TIMEOUT_MS = 5000;
    public static final long LONG_TIMEOUT_MS = 15000;

    public static String[] BAD_SUBJECTS_OR_QUEUES = new String[] {
        HAS_SPACE, HAS_CR, HAS_LF, HAS_TAB, STARTS_SPACE, ENDS_SPACE, null, EMPTY
    };

    public static ErrorListener NO_OP_EL = new ErrorListener() {};

    // ----------------------------------------------------------------------------------------------------
    // VersionCheck
    // ----------------------------------------------------------------------------------------------------
    public static ServerInfo RUN_SERVER_INFO;

    public static ServerInfo ensureRunServerInfo() throws Exception {
        if (RUN_SERVER_INFO == null) {
            _runInServer(false, null, null, nc -> {});
        }
        return RUN_SERVER_INFO;
    }

    public static void initRunServerInfo(Connection nc) {
        if (RUN_SERVER_INFO == null) {
            RUN_SERVER_INFO = nc.getServerInfo();
        }
    }

    public interface VersionCheck {
        boolean runTest(ServerInfo si);
    }

    public static boolean atLeast2_9_0() {
        return atLeast2_9_0(RUN_SERVER_INFO);
    }

    public static boolean atLeast2_9_0(Connection nc) {
        return atLeast2_9_0(nc.getServerInfo());
    }

    public static boolean atLeast2_9_0(ServerInfo si) {
        return si.isSameOrNewerThanVersion("2.9.0");
    }

    public static boolean atLeast2_10_26(ServerInfo si) {
        return si.isSameOrNewerThanVersion("2.10.26");
    }

    public static boolean atLeast2_9_1(ServerInfo si) {
        return si.isSameOrNewerThanVersion("2.9.1");
    }

    public static boolean atLeast2_10() {
        return atLeast2_10(RUN_SERVER_INFO);
    }

    public static boolean atLeast2_10(ServerInfo si) {
        return si.isNewerVersionThan("2.9.99");
    }

    public static boolean atLeast2_10_3(ServerInfo si) {
        return si.isSameOrNewerThanVersion("2.10.3");
    }

    public static boolean atLeast2_11() {
        return atLeast2_11(RUN_SERVER_INFO);
    }

    public static boolean atLeast2_11(ServerInfo si) {
        return si.isNewerVersionThan("2.10.99");
    }

    public static boolean before2_11() {
        return before2_11(RUN_SERVER_INFO);
    }

    public static boolean before2_11(ServerInfo si) {
        return si.isOlderThanVersion("2.11");
    }

    public static boolean atLeast2_12() {
        return atLeast2_12(RUN_SERVER_INFO);
    }

    public static boolean atLeast2_12(ServerInfo si) {
        return si.isSameOrNewerThanVersion("2.11.99");
    }

    // ----------------------------------------------------------------------------------------------------
    // runners / test interfaces
    // ----------------------------------------------------------------------------------------------------
    public interface InServerTest {
        void test(Connection nc) throws Exception;
    }

    public interface TwoServerTest {
        void test(Connection nc1, Connection nc2) throws Exception;
    }

    public interface ThreeServerTest {
        void test(Connection nc1, Connection nc2, Connection nc3) throws Exception;
    }

    public interface ThreeServerTestOptions {
        default void append(int index, Options.Builder builder) {}
        default boolean configureAccount() { return false; }
        default boolean includeAllServers() { return false; }
    }

    public interface InJetStreamTest {
        void test(Connection nc, JetStreamManagement jsm, JetStream js) throws Exception;
    }

    public interface InJetStreamTestingContextTest {
        void test(Connection nc, JetStreamTestingContext jstc) throws Exception;
    }

    // ----------------------------------------------------------------------------------------------------
    // runners / js cleanup
    // ----------------------------------------------------------------------------------------------------
    public static void cleanupJs(Connection c)
    {
        try {
            cleanupJs(c.jetStreamManagement());
        } catch (Exception ignore) {}
    }

    public static void cleanupJs(JetStreamManagement jsm)
    {
        try {
            List<String> streams = jsm.getStreamNames();
            for (String s : streams) {
                jsm.deleteStream(s);
            }
        } catch (Exception ignore) {}
    }

    // ----------------------------------------------------------------------------------------------------
    // runners -> new server
    // ----------------------------------------------------------------------------------------------------
    public static void runInServer(InServerTest inServerTest) throws Exception {
        _runInServer(false, null, null, inServerTest);
    }

    public static void runInServer(Options.Builder builder, InServerTest inServerTest) throws Exception {
        _runInServer(false, builder, null, inServerTest);
    }

    public static void runInJsServer(InServerTest inServerTest) throws Exception {
        _runInServer(true, null, null, inServerTest);
    }

    public static void runInJsServer(VersionCheck vc, InServerTest inServerTest) throws Exception {
        _runInServer(true, null, vc, inServerTest);
    }

    public static void runInJsServer(ErrorListener el, InServerTest inServerTest) throws Exception {
        _runInServer(true, Options.builder().errorListener(el), null, inServerTest);
    }

    public static void runInJsServer(ErrorListener el, VersionCheck vc, InServerTest inServerTest) throws Exception {
        _runInServer(true, Options.builder().errorListener(el), vc, inServerTest);
    }

    private static Options.Builder builder(ErrorListener el) {
        return Options.builder().errorListener(el);
    }

    // ----------------------------------------------------------------------------------------------------
    // runners -> long running server
    // ----------------------------------------------------------------------------------------------------
    public static void runInLrServer(InServerTest test) throws Exception {
        _runInLrServer(null, null, test, null, null);
    }

    public static void runInLrServerCloseableConnection(InServerTest test) throws Exception {
        _runInLrServer(LongRunningServer.optionsBuilder(), null, test, null, null);
    }

    public static void runInLrServer(Options.Builder builder, InServerTest test) throws Exception {
        _runInLrServer(builder, null, test, null, null);
    }

    public static void runInLrServer(InJetStreamTest jsTest) throws Exception {
        _runInLrServer(null, null, null, jsTest, null);
    }

    public static void runInLrServer(VersionCheck vc, InJetStreamTest jsTest) throws Exception {
        _runInLrServer(null, vc, null, jsTest, null);
    }

    public static void runInLrServer(ErrorListener el, InJetStreamTest jsTest) throws Exception {
        _runInLrServer(builder(el), null, null, jsTest, null);
    }

    public static void runInLrServer(Options.Builder builder, VersionCheck vc, InJetStreamTest jsTest) throws Exception {
        _runInLrServer(builder, vc, null, jsTest, null);
    }

    public static void runInLrServer(InJetStreamTestingContextTest oneSubjectJstcTest) throws Exception {
        _runInLrServer(null, null, null, null, oneSubjectJstcTest);
    }

    public static void runInLrServer(VersionCheck vc, InJetStreamTestingContextTest oneSubjectJstcTest) throws Exception {
        _runInLrServer(null, vc, null, null, oneSubjectJstcTest);
    }

    public static void runInLrServer(ErrorListener el, VersionCheck vc, InJetStreamTestingContextTest oneSubjectJstcTest) throws Exception {
        _runInLrServer(builder(el), vc, null, null, oneSubjectJstcTest);
    }

    public static void runInLrServer(ErrorListener el, InJetStreamTestingContextTest oneSubjectJstcTest) throws Exception {
        _runInLrServer(builder(el), null, null, null, oneSubjectJstcTest);
    }

    public static void runInLrServer(Options.Builder builder, InJetStreamTestingContextTest oneSubjectJstcTest) throws Exception {
        _runInLrServer(builder, null, null, null, oneSubjectJstcTest);
    }

    public static void runInLrServer(Options.Builder builder, VersionCheck vc, InJetStreamTestingContextTest oneSubjectJstcTest) throws Exception {
        _runInLrServer(builder, vc, null, null, oneSubjectJstcTest);
    }

    // ----------------------------------------------------------------------------------------------------
    // runners impl -> new server
    // runners impl -> long running server
    // ----------------------------------------------------------------------------------------------------
    private static void _runInServer(boolean jetstream, Options.Builder builder, VersionCheck vc, InServerTest inServerTest) throws Exception {
        if (vc != null && RUN_SERVER_INFO != null && !vc.runTest(RUN_SERVER_INFO)) {
            return; // had vc, already had run server info and fails check
        }

        try (NatsTestServer ts = new NatsTestServer(NatsTestServer.builder().jetstream(jetstream))) {
            if (builder == null) {
                builder = new Options.Builder().errorListener(NO_OP_EL);
            }

            try (Connection nc = standardConnectionWait(builder.server(ts.getURI()).build())) {
                initRunServerInfo(nc);
                if (vc == null || vc.runTest(RUN_SERVER_INFO)) {
                    try {
                        inServerTest.test(nc);
                    }
                    finally {
                        if (jetstream) {
                            cleanupJs(nc);
                        }
                    }
                }
            }
        }
    }

    private static void _runInLrServer(Options.Builder builder, VersionCheck vc,
                                       InServerTest test,
                                       InJetStreamTest jsTest,
                                       InJetStreamTestingContextTest oneSubjectJstcTest) throws Exception {
        if (vc != null && RUN_SERVER_INFO != null && !vc.runTest(RUN_SERVER_INFO)) {
            return; // had vc, already had run server info and fails check
        }

        // no builder, we can use the long-running connection since it's totally generic
        // with a builder, just make a fresh connection and close it at the end.
        boolean closeWhenDone;
        Connection nc;
        if (builder == null) {
            closeWhenDone = false;
            nc = LongRunningServer.getLrConn();
        }
        else {
            closeWhenDone = true;
            nc = longConnectionWait(builder.server(LongRunningServer.uri()).build());
        }

        initRunServerInfo(nc);
        if (vc == null || vc.runTest(RUN_SERVER_INFO)) {
            try {
                if (oneSubjectJstcTest != null) {
                    try (JetStreamTestingContext jstc = new JetStreamTestingContext(nc, 1)) {
                        oneSubjectJstcTest.test(nc, jstc);
                    }
                }
                else if (jsTest != null) {
                    NatsJetStreamManagement jsm = (NatsJetStreamManagement) nc.jetStreamManagement();
                    NatsJetStream js = (NatsJetStream) nc.jetStream();
                    jsTest.test(nc, jsm, js);
                }
                else {
                    test.test(nc);
                }
            }
            finally {
                if (test == null) {
                    cleanupJs(nc);
                }
                if (closeWhenDone) {
                    try {
                        nc.close();
                    }
                    catch (Exception ignore) {}
                }
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // runners / external
    // ----------------------------------------------------------------------------------------------------
    public static void runInExternalServer(InServerTest inServerTest) throws Exception {
        runInExternalServer(Options.DEFAULT_URL, inServerTest);
    }

    public static void runInExternalServer(String url, InServerTest inServerTest) throws Exception {
        try (Connection nc = Nats.connect(url)) {
            inServerTest.test(nc);
        }
    }


    // ----------------------------------------------------------------------------------------------------
    // runners / special
    // ----------------------------------------------------------------------------------------------------
    public static String HUB_DOMAIN = "HUB";
    public static String LEAF_DOMAIN = "LEAF";

    public static void runInJsHubLeaf(TwoServerTest twoServerTest) throws Exception {
        int hubPort = NatsTestServer.nextPort();
        int hubLeafPort = NatsTestServer.nextPort();
        int leafPort = NatsTestServer.nextPort();

        String[] hubInserts = new String[] {
            "server_name: " + HUB_DOMAIN,
            "jetstream {",
            "    store_dir: " + tempJsStoreDir(),
            "    domain: " + HUB_DOMAIN,
            "}",
            "leafnodes {",
            "  listen = 127.0.0.1:" + hubLeafPort,
            "}"
        };

        String[] leafInserts = new String[] {
            "server_name: " + LEAF_DOMAIN,
            "jetstream {",
            "    store_dir: " + tempJsStoreDir(),
            "    domain: " + LEAF_DOMAIN,
            "}",
            "leafnodes {",
            "  remotes = [ { url: \"leaf://127.0.0.1:" + hubLeafPort + "\" } ]",
            "}"
        };

        try (NatsTestServer hub = new NatsTestServer(hubPort, false, true, null, hubInserts, null);
             Connection nchub = standardConnectionWait(hub.getURI());
             NatsTestServer leaf = new NatsTestServer(leafPort, false, true, null, leafInserts, null);
             Connection ncleaf = standardConnectionWait(leaf.getURI())
        ) {
            try {
                twoServerTest.test(nchub, ncleaf);
            }
            finally {
                cleanupJs(nchub);
                cleanupJs(ncleaf);
            }
        }
    }

    public static void runInJsCluster(ThreeServerTest threeServerTest) throws Exception {
        runInJsCluster(null, threeServerTest);
    }

    public static void runInJsCluster(ThreeServerTestOptions tstOpts, ThreeServerTest threeServerTest) throws Exception {
        int port1 = NatsTestServer.nextPort();
        int port2 = NatsTestServer.nextPort();
        int port3 = NatsTestServer.nextPort();
        int listen1 = NatsTestServer.nextPort();
        int listen2 = NatsTestServer.nextPort();
        int listen3 = NatsTestServer.nextPort();
        String dir1 = tempJsStoreDir();
        String dir2 = tempJsStoreDir();
        String dir3 = tempJsStoreDir();
        String cluster = "cluster_" + random();
        String serverPrefix = "server_" + random() + "_";

        if (tstOpts == null) {
            tstOpts = new ThreeServerTestOptions() {};
        }
        boolean configureAccount = tstOpts.configureAccount();

        String[] server1Inserts = makeInsert(cluster, serverPrefix + 1, dir1, listen1, listen2, listen3, configureAccount);
        String[] server2Inserts = makeInsert(cluster, serverPrefix + 2, dir2, listen2, listen1, listen3, configureAccount);
        String[] server3Inserts = makeInsert(cluster, serverPrefix + 3, dir3, listen3, listen1, listen2, configureAccount);

        try (NatsTestServer srv1 = new NatsTestServer(port1, false, true, null, server1Inserts, null);
             NatsTestServer srv2 = new NatsTestServer(port2, false, true, null, server2Inserts, null);
             NatsTestServer srv3 = new NatsTestServer(port3, false, true, null, server3Inserts, null);
             Connection nc1 = standardConnectionWait(makeOptions(0, tstOpts, srv1, srv2, srv3));
             Connection nc2 = standardConnectionWait(makeOptions(1, tstOpts, srv2, srv1, srv3));
             Connection nc3 = standardConnectionWait(makeOptions(2, tstOpts, srv3, srv1, srv2))
        ) {
            try {
                threeServerTest.test(nc1, nc2, nc3);
            }
            finally {
                cleanupJs(nc1);
                cleanupJs(nc2);
                cleanupJs(nc3);
            }
        }
    }

    private static String[] makeInsert(String clusterName, String serverName, String jsStoreDir, int listen, int route1, int route2, boolean configureAccount) {
        String[] serverInserts = new String[configureAccount ? 19 : 12];
        int x = -1;
        serverInserts[++x] = "jetstream {";
        serverInserts[++x] = "    store_dir=" + jsStoreDir;
        serverInserts[++x] = "}";
        serverInserts[++x] = "server_name=" + serverName;
        serverInserts[++x] = "cluster {";
        serverInserts[++x] = "  name: " + clusterName;
        serverInserts[++x] = "  listen: 127.0.0.1:" + listen;
        serverInserts[++x] = "  routes: [";
        serverInserts[++x] = "    nats-route://127.0.0.1:" + route1;
        serverInserts[++x] = "    nats-route://127.0.0.1:" + route2;
        serverInserts[++x] = "  ]";
        serverInserts[++x] = "}";
        if (configureAccount) {
            serverInserts[++x] = "accounts {";
            serverInserts[++x] = "  $SYS: {}";
            serverInserts[++x] = "  NVCF: {";
            serverInserts[++x] = "    jetstream: \"enabled\",";
            serverInserts[++x] = "    users: [ { nkey: " + USER_NKEY + " } ]";
            serverInserts[++x] = "  }";
            serverInserts[++x] = "}";
        }
        return serverInserts;
    }

    private static final String USER_NKEY = "UBAX6GCZQYLJDLSNPBDDPLY6KIBRO2JAUYNPW4HCWBRCZ4OU57YQQQS3";
    private static final String USER_SEED = "SUAIUIHFQNVWSMKYGC4E5H5IEQZHHND3DKHTRKZWPCDXB6LXVD5R2KROSA";

    private static Options makeOptions(int id, ThreeServerTestOptions tstOpts, NatsTestServer... srvs) {
        Options.Builder b = Options.builder();
        if (tstOpts.includeAllServers()) {
            String[] servers = new String[srvs.length];
            for (int i = 0; i < srvs.length; i++) {
                NatsTestServer nts = srvs[i];
                servers[i] = nts.getURI();
            }
            b.servers(servers);
        }
        else {
            b.server(srvs[0].getURI());
        }
        if (tstOpts.configureAccount()) {
            b.authHandler(Nats.staticCredentials(null, USER_SEED.toCharArray()));
        }
        tstOpts.append(id, b);
        return b.build();
    }

    private static String tempJsStoreDir() throws IOException {
        return Files.createTempDirectory(random()).toString().replace("\\", "\\\\");  // when on windows this is necessary. unix doesn't have backslash
    }

    // ----------------------------------------------------------------------------------------------------
    // data makers
    // ----------------------------------------------------------------------------------------------------
    public static final String DATA = "data";

    public static String random() {
        return NUID.nextGlobalSequence();
    }

    public static String randomWide(int width) {
        StringBuilder sb = new StringBuilder();
        while (sb.length() < width) {
            sb.append(random());
        }
        return sb.substring(0, width);
    }

    public static String subject(int variant) {
        return "subject-" + variant;
    }

    public static String subjectGt(String subject) {
        return subject + ".>";
    }

    public static String subjectStar(String subject) {
        return subject + ".*";
    }

    public static String subjectDot(String subject, String field) {
        return subject + DOT + field;
    }

    public static String data(Object variant) {
        return DATA + "-" + variant;
    }

    public static byte[] dataBytes() {
        return data(random()).getBytes(StandardCharsets.US_ASCII);
    }
    public static byte[] dataBytes(Object variant) {
        return data(variant).getBytes(StandardCharsets.US_ASCII);
    }

    public static NatsMessage getDataMessage(String data) {
        return new NatsMessage(random(), null, data.getBytes(StandardCharsets.US_ASCII));
    }

    // ----------------------------------------------------------------------------------------------------
    // assertions
    // ----------------------------------------------------------------------------------------------------
    public static void assertConnected(Connection conn) {
        assertSame(Connection.Status.CONNECTED, conn.getStatus(),
                () -> expectingMessage(conn, Connection.Status.CONNECTED));
    }

    public static void assertNotConnected(Connection conn) {
        assertNotSame(Connection.Status.CONNECTED, conn.getStatus(),
                () -> "Failed not expecting Connection Status " + Connection.Status.CONNECTED.name());
    }

    public static void assertClosed(Connection conn) {
        assertSame(Connection.Status.CLOSED, conn.getStatus(),
                () -> expectingMessage(conn, Connection.Status.CLOSED));
    }

    public static void assertCanConnect() throws IOException, InterruptedException {
        standardCloseConnection( standardConnectionWait() );
    }

    public static void assertCanConnect(String serverURL) throws IOException, InterruptedException {
        standardCloseConnection( standardConnectionWait(serverURL) );
    }

    public static void assertCanConnect(Options options) throws IOException, InterruptedException {
        standardCloseConnection( standardConnectionWait(options) );
    }

    public static void assertCanConnectAndPubSub() throws IOException, InterruptedException {
        Connection conn = standardConnectionWait();
        assertPubSub(conn);
        standardCloseConnection(conn);
    }

    public static void assertCanConnectAndPubSub(String serverURL) throws IOException, InterruptedException {
        Connection conn = standardConnectionWait(serverURL);
        assertPubSub(conn);
        standardCloseConnection(conn);
    }

    public static void assertCanConnectAndPubSub(Options options) throws IOException, InterruptedException {
        Connection conn = standardConnectionWait(options);
        assertPubSub(conn);
        standardCloseConnection(conn);
    }

    public static void assertByteArraysEqual(byte[] data1, byte[] data2) {
        if (data1 == null) {
            assertNull(data2);
            return;
        }
        assertNotNull(data2);
        assertEquals(data1.length, data2.length);
        for (int x = 0; x < data1.length; x++) {
            assertEquals(data1[x], data2[x]);
        }
    }

    public static void assertPubSub(Connection conn) throws InterruptedException {
        String subject = random();
        String data = data(null);
        Subscription sub = conn.subscribe(subject);
        conn.publish(subject, data.getBytes());
        Message m = sub.nextMessage(Duration.ofSeconds(2));
        assertNotNull(m);
        assertEquals(data, new String(m.getData()));
    }

    // ----------------------------------------------------------------------------------------------------
    // utils / macro utils
    // ----------------------------------------------------------------------------------------------------
    public static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) { /* ignored */ }
    }

    public static void park(Duration d) {
        try { LockSupport.parkNanos(d.toNanos()); } catch (Exception ignored) { /* ignored */ }
    }

    public static void debugPrintln(Object... debug) {
        StringBuilder sb = new StringBuilder();
        sb.append(System.currentTimeMillis());
        sb.append(" [");
        sb.append(Thread.currentThread().getName());
        sb.append(",");
        sb.append(Thread.currentThread().getPriority());
        sb.append("] ");
        boolean flag = true;
        for (Object o : debug) {
            if (flag) {
                flag = false;
            }
            else {
                sb.append(" | ");
            }
            sb.append(o);
        }
        System.out.println(sb.toString());
    }

    // ----------------------------------------------------------------------------------------------------
    // flush
    // ----------------------------------------------------------------------------------------------------
    public static void flushConnection(Connection conn) {
        flushConnection(conn, Duration.ofMillis(STANDARD_FLUSH_TIMEOUT_MS));
    }

    public static void flushConnection(Connection conn, long timeoutMillis) {
        flushConnection(conn, Duration.ofMillis(timeoutMillis));
    }

    public static void flushConnection(Connection conn, Duration timeout) {
        try { conn.flush(timeout); } catch (Exception exp) { /* ignored */ }
    }

    public static void flushAndWait(Connection conn, ListenerForTesting listener, long flushTimeoutMillis, long waitForStatusMillis) {
        flushConnection(conn, flushTimeoutMillis);
        listener.waitForStatusChange(waitForStatusMillis, TimeUnit.MILLISECONDS);
    }

    public static void flushAndWaitLong(Connection conn, ListenerForTesting listener) {
        flushAndWait(conn, listener, STANDARD_FLUSH_TIMEOUT_MS, LONG_TIMEOUT_MS);
    }

    // ----------------------------------------------------------------------------------------------------
    // connect or wait for a connection
    // ----------------------------------------------------------------------------------------------------
    public static Options.Builder standardOptionsBuilder() {
        return Options.builder().reportNoResponders().errorListener(new ListenerForTesting());
    }

    public static Options.Builder standardOptionsBuilder(String serverURL) {
        return standardOptionsBuilder().server(serverURL);
    }

    public static Options standardOptions() {
        return standardOptionsBuilder().build();
    }

    public static Options standardOptions(String serverURL) {
        return standardOptionsBuilder(serverURL).build();
    }

    public static Connection connectionWait(Connection conn, long millis) {
        return waitUntilStatus(conn, millis, Connection.Status.CONNECTED);
    }

    public static Connection standardConnectionWait() throws IOException, InterruptedException {
        return standardConnectionWait( Nats.connect(standardOptions()) );
    }

    public static Connection standardConnectionWait(String serverURL) throws IOException, InterruptedException {
        return connectionWait(Nats.connect(standardOptions(serverURL)), STANDARD_CONNECTION_WAIT_MS);
    }

    public static Connection standardConnectionWait(Options options) throws IOException, InterruptedException {
        return connectionWait(Nats.connect(options), STANDARD_CONNECTION_WAIT_MS);
    }

    public static Connection standardConnectionWait(Connection conn) {
        return connectionWait(conn, STANDARD_CONNECTION_WAIT_MS);
    }

    public static Connection longConnectionWait(String serverURL) throws IOException, InterruptedException {
        return connectionWait(Nats.connect(standardOptions(serverURL)), LONG_CONNECTION_WAIT_MS);
    }

    public static Connection longConnectionWait(Options options) throws IOException, InterruptedException {
        return connectionWait(Nats.connect(options), LONG_CONNECTION_WAIT_MS);
    }

    public static Connection listenerConnectionWait(Options options, ListenerForTesting listener) throws IOException, InterruptedException {
        Connection conn = Nats.connect(options);
        listenerConnectionWait(conn, listener, LONG_CONNECTION_WAIT_MS);
        return conn;
    }

    public static void listenerConnectionWait(Connection conn, ListenerForTesting listener) {
        listenerConnectionWait(conn, listener, LONG_CONNECTION_WAIT_MS);
    }

    public static void listenerConnectionWait(Connection conn, ListenerForTesting listener, long millis) {
        listener.waitForStatusChange(millis, TimeUnit.MILLISECONDS);
        assertConnected(conn);
    }

    // ----------------------------------------------------------------------------------------------------
    // close
    // ----------------------------------------------------------------------------------------------------
    public static void standardCloseConnection(Connection conn) {
        closeConnection(conn, STANDARD_CONNECTION_WAIT_MS);
    }

    public static void closeConnection(Connection conn, long millis) {
        if (conn != null) {
            close(conn);
            waitUntilStatus(conn, millis, Connection.Status.CLOSED);
            assertClosed(conn);
        }
    }

    public static void close(Connection conn) {
        try { conn.close(); } catch (InterruptedException e) { /* ignored */ }
    }

    // ----------------------------------------------------------------------------------------------------
    // connection waiting
    // ----------------------------------------------------------------------------------------------------
    public static Connection waitUntilStatus(Connection conn, long millis, Connection.Status waitUntilStatus) {
        long times = (millis + 99) / 100;
        for (long x = 0; x < times; x++) {
            sleep(100);
            if (conn.getStatus() == waitUntilStatus) {
                return conn;
            }
        }

        throw new AssertionFailedError(expectingMessage(conn, waitUntilStatus));
    }

    private static String expectingMessage(Connection conn, Connection.Status expecting) {
        return "Failed expecting Connection Status " + expecting.name() + " but was " + conn.getStatus();
    }

    public static void assertTrueByTimeout(long millis, Supplier<Boolean> test) {
        long times = (millis + 99) / 100;
        for (long x = 0; x < times; x++) {
            sleep(100);
            if (test.get()) {
                return;
            }
        }
        fail();
    }

    // ----------------------------------------------------------------------------------------------------
    // Subscription or test macros
    // ----------------------------------------------------------------------------------------------------
    public void assertClientError(NatsJetStreamClientError error, Executable executable) {
        Exception e = assertThrows(Exception.class, executable);
        assertTrue(e.getMessage().contains(error.id()));
        if (error.getKind() == KIND_ILLEGAL_ARGUMENT) {
            assertInstanceOf(IllegalArgumentException.class, e);
        }
        else if (error.getKind() == KIND_ILLEGAL_STATE) {
            assertInstanceOf(IllegalStateException.class, e);
        }
    }

    public static void assertMetaData(Map<String, String> metadata) {
        if (atLeast2_10()) {
            if (before2_11()) {
                assertEquals(1, metadata.size());
            }
            else {
                assertTrue(metadata.size() > 1);
            }
            assertEquals(META_VALUE, metadata.get(META_KEY));
        }
        else if (atLeast2_9_0() ){
            assertNotNull(metadata);
            assertEquals(0, metadata.size());
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // JetStream JetStream JetStream JetStream JetStream JetStream JetStream JetStream JetStream JetStream
    // ----------------------------------------------------------------------------------------------------
    public static StreamInfo createMemoryStream(JetStreamManagement jsm, String streamName, String... subjects) throws IOException, JetStreamApiException {
        if (streamName == null) {
            streamName = random();
        }

        if (subjects == null || subjects.length == 0) {
            subjects = new String[]{random()};
        }

        StreamConfiguration sc = StreamConfiguration.builder()
            .name(streamName)
            .storageType(StorageType.Memory)
            .subjects(subjects).build();

        return jsm.addStream(sc);
    }

    public static StreamInfo createMemoryStream(Connection nc, String streamName, String... subjects)
        throws IOException, JetStreamApiException {
        return createMemoryStream(nc.jetStreamManagement(), streamName, subjects);
    }
}
