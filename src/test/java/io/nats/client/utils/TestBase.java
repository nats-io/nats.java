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

import io.nats.NatsServerRunner;
import io.nats.client.*;
import io.nats.client.api.ServerInfo;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.*;
import io.nats.client.support.NatsJetStreamClientError;
import org.junit.jupiter.api.function.Executable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.nats.client.support.NatsConstants.DOT;
import static io.nats.client.support.NatsConstants.EMPTY;
import static io.nats.client.support.NatsJetStreamClientError.KIND_ILLEGAL_ARGUMENT;
import static io.nats.client.support.NatsJetStreamClientError.KIND_ILLEGAL_STATE;
import static io.nats.client.utils.ConnectionUtils.*;
import static io.nats.client.utils.OptionsUtils.options;
import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static io.nats.client.utils.ResourceUtils.configResource;
import static io.nats.client.utils.ThreadUtils.sleep;
import static io.nats.client.utils.VersionUtils.*;
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

    public static String[] BAD_SUBJECTS_OR_QUEUES = new String[] {
        HAS_SPACE, HAS_CR, HAS_LF, HAS_TAB, STARTS_SPACE, ENDS_SPACE, null, EMPTY
    };

    public static ServerInfo ensureVersionServerInfo() throws Exception {
        if (VERSION_SERVER_INFO == null) {
            runInShared(VersionUtils::initVersionServerInfo);
        }
        return VERSION_SERVER_INFO;
    }

    // ----------------------------------------------------------------------------------------------------
    // runners / test interfaces
    // ----------------------------------------------------------------------------------------------------
    public interface OneConnectionTest {
        void test(Connection nc) throws Exception;
    }

    public interface TwoConnectionTest {
        void test(Connection nc1, Connection nc2) throws Exception;
    }

    public interface ThreeConnectionTest {
        void test(Connection nc1, Connection nc2, Connection nc3) throws Exception;
    }

    public interface ThreeServerTestOptions {
        default void append(int index, Options.Builder builder) {}
        default boolean configureAccount() { return false; }
        default boolean includeAllServers() { return false; }
        default boolean jetStream() { return false; }
    }

    public interface JetStreamTest {
        void test(Connection nc, JetStreamManagement jsm, JetStream js) throws Exception;
    }

    public interface JetStreamTestingContextTest {
        void test(Connection nc, JetStreamTestingContext ctx) throws Exception;
    }

    // ----------------------------------------------------------------------------------------------------
    // runners -> own server
    // ----------------------------------------------------------------------------------------------------
    private static void _runInOwnServer(
        Options.Builder optionsBuilder,
        VersionCheck vc,
        String configFilePath,
        OneConnectionTest oneNcTest,
        JetStreamTest jsTest
    ) throws Exception {
        if (vc != null && VERSION_SERVER_INFO != null && !vc.runTest(VERSION_SERVER_INFO)) {
            return; // had vc, already had run server info and fails check
        }

        NatsServerRunner.Builder nsrb = NatsServerRunner.builder().jetstream(jsTest != null);
        if (configFilePath != null) {
            nsrb.configFilePath(configResource(configFilePath));
        }
        try (NatsTestServer ts = new NatsTestServer(nsrb)) {
            Options options = (optionsBuilder == null
                ? optionsBuilder(ts)
                : optionsBuilder.server(ts.getServerUri()))
                .build();
            try (Connection nc = standardConnectionWait(options)) {
                initVersionServerInfo(nc);
                if (vc == null || vc.runTest(VERSION_SERVER_INFO)) {
                    if (jsTest == null) {
                        oneNcTest.test(nc);
                    }
                    else {
                        NatsJetStreamManagement jsm = (NatsJetStreamManagement) nc.jetStreamManagement();
                        NatsJetStream js = (NatsJetStream) nc.jetStream();
                        jsTest.test(nc, jsm, js);
                    }
                }
            }
        }
    }

    // --------------------------------------------------
    // Not using JetStream
    // --------------------------------------------------
    public static void runInOwnServer(OneConnectionTest oneNcTest) throws Exception {
        _runInOwnServer(null, null, null, oneNcTest, null);
    }

    // --------------------------------------------------
    // JetStream needing isolation
    // --------------------------------------------------
    public static void runInOwnJsServer(JetStreamTest jetStreamTest) throws Exception {
        _runInOwnServer(null, null, null, null, jetStreamTest);
    }

    public static void runInOwnJsServer(ErrorListener el, JetStreamTest jetStreamTest) throws Exception {
        _runInOwnServer(optionsBuilder(el), null, null, null, jetStreamTest);
    }

    public static void runInOwnJsServer(String configFilePath, JetStreamTest jetStreamTest) throws Exception {
        _runInOwnServer(null, null, configFilePath, null, jetStreamTest);
    }

    public static void runInOwnJsServer(VersionCheck vc, JetStreamTest jetStreamTest) throws Exception {
        _runInOwnServer(null, vc, null, null, jetStreamTest);
    }

    // ----------------------------------------------------------------------------------------------------
    // runners -> shared
    // ----------------------------------------------------------------------------------------------------
    private static void _runInShared(
        Options.Builder optionsBuilder,
        VersionCheck vc,
        OneConnectionTest oneNcTest,
        int jstcTestSubjectCount,
        JetStreamTestingContextTest ctxTest
    ) throws Exception {
        if (vc != null && VERSION_SERVER_INFO != null && !vc.runTest(VERSION_SERVER_INFO)) {
            return; // had vc, already had run server info and fails check
        }

        SharedServer shared = SharedServer.getInstance("shared");

        // no builder, we can use the long-running connection since it's totally generic
        // with a builder, just make a fresh connection and close it at the end.
        boolean closeNcWhenDone;
        Connection nc;
        if (optionsBuilder == null) {
            closeNcWhenDone = false;
            nc = shared.getSharedConnection();
        }
        else {
            closeNcWhenDone = true;
            nc = shared.newConnection(optionsBuilder);
            if (nc == null) {
                throw new RuntimeException("Unable to open a new connection to reusable sever.");
            }
        }

        initVersionServerInfo(nc);
        if (vc == null || vc.runTest(VERSION_SERVER_INFO)) {
            try {
                if (ctxTest != null) {
                    try (JetStreamTestingContext ctx = new JetStreamTestingContext(nc, jstcTestSubjectCount)) {
                        ctxTest.test(nc, ctx);
                    }
                }
                else {
                    oneNcTest.test(nc);
                }
            }
            finally {
                if (closeNcWhenDone) {
                    try { nc.close(); } catch (Exception ignore) {}
                }
            }
        }
    }

    // --------------------------------------------------
    // 1. Not using JetStream
    // -or-
    // 2. need something very custom ->
    //    please clean up your streams
    // -or-
    // 3. or need to do something special with the
    //    connection list close it
    // --------------------------------------------------
    public static void runInShared(OneConnectionTest test) throws Exception {
        _runInShared(null, null, test, -1, null);
    }

    public static void runInShared(VersionCheck vc, OneConnectionTest test) throws Exception {
        _runInShared(null, vc, test, -1, null);
    }

    public static void runInSharedOwnNc(OneConnectionTest test) throws Exception {
        _runInShared(optionsBuilder(), null, test, -1, null);
    }

    public static void runInSharedOwnNc(ErrorListener el, OneConnectionTest test) throws Exception {
        _runInShared(optionsBuilder(el), null, test, -1, null);
    }

    public static void runInSharedOwnNc(Options.Builder builder, OneConnectionTest test) throws Exception {
        _runInShared(builder, null, test, -1, null);
    }

    // --------------------------------------------------
    // JetStream: 1 stream 1 subject
    // --------------------------------------------------
    public static void runInShared(JetStreamTestingContextTest ctxTest) throws Exception {
        _runInShared(null, null, null, 1, ctxTest);
    }

    public static void runInShared(VersionCheck vc, JetStreamTestingContextTest ctxTest) throws Exception {
        _runInShared(null, vc, null, 1, ctxTest);
    }

    public static void runInSharedOwnNc(ErrorListener el, JetStreamTestingContextTest ctxTest) throws Exception {
        _runInShared(optionsBuilder(el), null, null, 1, ctxTest);
    }

    public static void runInSharedOwnNc(ErrorListener el, VersionCheck vc, JetStreamTestingContextTest ctxTest) throws Exception {
        _runInShared(optionsBuilder(el), vc, null, 1, ctxTest);
    }

    public static void runInSharedOwnNc(Options.Builder builder, JetStreamTestingContextTest ctxTest) throws Exception {
        _runInShared(builder, null, null, 1, ctxTest);
    }

    public static void runInSharedOwnNc(Options.Builder builder, VersionCheck vc, JetStreamTestingContextTest ctxTest) throws Exception {
        _runInShared(builder, vc, null, 1, ctxTest);
    }

    // --------------------------------------------------
    // JetStream: 1 stream custom subjects, kv or os
    // --------------------------------------------------
    public static void runInSharedCustom(JetStreamTestingContextTest ctxTest) throws Exception {
        _runInShared(null, null, null, 0, ctxTest);
    }

    public static void runInSharedCustom(VersionCheck vc, JetStreamTestingContextTest ctxTest) throws Exception {
        _runInShared(null, vc, null, 0, ctxTest);
    }

    public static void runInSharedCustom(Options.Builder builder, JetStreamTestingContextTest ctxTest) throws Exception {
        _runInShared(builder, null, null, -1, ctxTest);
    }

    // ----------------------------------------------------------------------------------------------------
    // runners / external
    // ----------------------------------------------------------------------------------------------------
    public static void runInExternalServer(OneConnectionTest oneNcTest) throws Exception {
        runInExternalServer(Options.DEFAULT_URL, oneNcTest);
    }

    public static void runInExternalServer(String url, OneConnectionTest oneNcTest) throws Exception {
        try (Connection nc = Nats.connect(url)) {
            oneNcTest.test(nc);
        }
    }


    // ----------------------------------------------------------------------------------------------------
    // runners / special
    // ----------------------------------------------------------------------------------------------------
    public static String HUB_DOMAIN = "HUB";
    public static String LEAF_DOMAIN = "LEAF";

    public static void runInJsHubLeaf(TwoConnectionTest twoConnectionTest) throws Exception {
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

        try (NatsTestServer hub = new NatsTestServer(hubPort, true, null, hubInserts, null);
             Connection nchub = standardConnectionWait(options(hub));
             NatsTestServer leaf = new NatsTestServer(leafPort, true, null, leafInserts, null);
             Connection ncleaf = standardConnectionWait(options(leaf))
        ) {
            twoConnectionTest.test(nchub, ncleaf);
        }
    }

    public static void runInCluster(ThreeConnectionTest threeServerTest) throws Exception {
        runInCluster(null, threeServerTest);
    }

    public static void runInCluster(ThreeServerTestOptions tstOpts, ThreeConnectionTest threeServerTest) throws Exception {
        if (tstOpts == null) {
            tstOpts = new ThreeServerTestOptions() {};
        }

        int port1 = NatsTestServer.nextPort();
        int port2 = NatsTestServer.nextPort();
        int port3 = NatsTestServer.nextPort();
        int listen1 = NatsTestServer.nextPort();
        int listen2 = NatsTestServer.nextPort();
        int listen3 = NatsTestServer.nextPort();
        String dir1 = tstOpts.jetStream() ? tempJsStoreDir() : null;
        String dir2 = tstOpts.jetStream() ? tempJsStoreDir() : null;
        String dir3 = tstOpts.jetStream() ? tempJsStoreDir() : null;
        String cluster = "cluster_" + random();
        String serverPrefix = "server_" + random() + "_";

        boolean configureAccount = tstOpts.configureAccount();

        String[] server1Inserts = makeInsert(cluster, serverPrefix + 1, dir1, listen1, listen2, listen3, configureAccount);
        String[] server2Inserts = makeInsert(cluster, serverPrefix + 2, dir2, listen2, listen1, listen3, configureAccount);
        String[] server3Inserts = makeInsert(cluster, serverPrefix + 3, dir3, listen3, listen1, listen2, configureAccount);

        try (NatsTestServer srv1 = new NatsTestServer(port1, tstOpts.jetStream(), null, server1Inserts, null);
             NatsTestServer srv2 = new NatsTestServer(port2, tstOpts.jetStream(), null, server2Inserts, null);
             NatsTestServer srv3 = new NatsTestServer(port3, tstOpts.jetStream(), null, server3Inserts, null);
             Connection nc1 = standardConnectionWait(makeOptions(0, tstOpts, srv1, srv2, srv3));
             Connection nc2 = standardConnectionWait(makeOptions(1, tstOpts, srv2, srv1, srv3));
             Connection nc3 = standardConnectionWait(makeOptions(2, tstOpts, srv3, srv1, srv2))
        ) {
            threeServerTest.test(nc1, nc2, nc3);
        }
    }

    private static String[] makeInsert(String clusterName, String serverName, String jsStoreDir, int listen, int route1, int route2, boolean configureAccount) {
        List<String> serverInserts = new ArrayList<>();
        if (jsStoreDir != null) {
            serverInserts.add("jetstream {");
            serverInserts.add("    store_dir=" + jsStoreDir);
            serverInserts.add("}");
        }
        serverInserts.add("server_name=" + serverName);
        serverInserts.add("cluster {");
        serverInserts.add("  name: " + clusterName);
        serverInserts.add("  listen: 127.0.0.1:" + listen);
        serverInserts.add("  routes: [");
        serverInserts.add("    nats-route://127.0.0.1:" + route1);
        serverInserts.add("    nats-route://127.0.0.1:" + route2);
        serverInserts.add("  ]");
        serverInserts.add("}");
        if (configureAccount) {
            serverInserts.add("accounts {");
            serverInserts.add("  $SYS: {}");
            serverInserts.add("  NVCF: {");
            if (jsStoreDir != null) {
                serverInserts.add("    jetstream: \"enabled\",");
            }
            serverInserts.add("    users: [ { nkey: " + USER_NKEY + " } ]");
            serverInserts.add("  }");
            serverInserts.add("}");
        }
        return serverInserts.toArray(new String[0]);
    }

    private static final String USER_NKEY = "UBAX6GCZQYLJDLSNPBDDPLY6KIBRO2JAUYNPW4HCWBRCZ4OU57YQQQS3";
    private static final String USER_SEED = "SUAIUIHFQNVWSMKYGC4E5H5IEQZHHND3DKHTRKZWPCDXB6LXVD5R2KROSA";

    private static Options makeOptions(int id, ThreeServerTestOptions tstOpts, NatsTestServer... srvs) {
        Options.Builder b = Options.builder();
        if (tstOpts.includeAllServers()) {
            String[] servers = new String[srvs.length];
            for (int i = 0; i < srvs.length; i++) {
                NatsTestServer nts = srvs[i];
                servers[i] = nts.getServerUri();
            }
            b.servers(servers);
        }
        else {
            b.server(srvs[0].getServerUri());
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
    public static void createMemoryStream(JetStreamManagement jsm, String streamName, String... subjects) throws IOException, JetStreamApiException {
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

        jsm.addStream(sc);
    }
}
