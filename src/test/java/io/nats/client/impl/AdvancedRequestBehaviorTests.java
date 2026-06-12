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

package io.nats.client.impl;

import io.nats.client.*;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests (real test server) proving the opt-in {@code advancedRequestBehavior} flag.
 * When the flag is ON, a JetStream request that comes back without a response throws a typed
 * {@link RequestFailureException} carrying a classified {@link RequestFailureReason}.
 * When the flag is OFF, behavior is byte-for-byte unchanged (the legacy generic IOException).
 *
 * The companion unit test {@code OptionsTests.testAdvancedRequestBehavior} already covers the
 * option plumbing (default/builder/copy/property); these tests cover the end-to-end runtime behavior.
 */
public class AdvancedRequestBehaviorTests extends TestBase {

    // legacy message thrown by NatsJetStreamImpl.responseRequired when the flag is OFF
    private static final String LEGACY_NO_RESPONSE_MSG = "Timeout or no response waiting for NATS JetStream server";

    // ----------------------------------------------------------------------------------------------------
    // NO_RESPONDERS - a JetStream management request against a server with no JetStream responder.
    // The connection's cancelAction defaults to CANCEL (reportNoResponders not set), so a 503
    // no-responders cancels the in-flight future -> CancellationException, not timed out -> NO_RESPONDERS.
    // ----------------------------------------------------------------------------------------------------
    @Test
    public void testNoRespondersReasonWhenFlagOn() throws Exception {
        // runInServer starts a plain (non JetStream) server, so the $JS.API.* subjects have no responder at all.
        runInServer(Options.builder().advancedRequestBehavior(), nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            RequestFailureException e = assertThrows(RequestFailureException.class,
                () -> jsm.getStreamInfo("no-such-stream-anywhere"));

            assertEquals(RequestFailureReason.NO_RESPONDERS, e.getReason());
            assertEquals(Connection.Status.CONNECTED, e.getConnectionStatus());
            // no protocol -ERR was involved
            assertTrue(e.getLastError() == null || e.getLastError().isEmpty());
        });
    }

    // ----------------------------------------------------------------------------------------------------
    // Regression guard: SAME scenario with the flag OFF must still throw the plain legacy IOException
    // (and must NOT be the typed subclass).
    // ----------------------------------------------------------------------------------------------------
    @Test
    public void testLegacyIOExceptionWhenFlagOff() throws Exception {
        // default options: advancedRequestBehavior intentionally NOT set
        runInServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            IOException e = assertThrows(IOException.class,
                () -> jsm.getStreamInfo("no-such-stream-anywhere"));

            assertFalse(e instanceof RequestFailureException,
                "With the flag OFF the typed exception must not be thrown");
            assertEquals(LEGACY_NO_RESPONSE_MSG, e.getMessage());
        });
    }

    // ----------------------------------------------------------------------------------------------------
    // TIMEOUT - a responder IS subscribed to the JetStream API subject (so it is not a 503 no-responders),
    // but it never replies. With a short JetStream request timeout the future times out
    // (TimeoutException, not cancelled-closing, no -ERR) -> TIMEOUT.
    // ----------------------------------------------------------------------------------------------------
    @Test
    public void testTimeoutReasonWhenFlagOn() throws Exception {
        runInServer(Options.builder().advancedRequestBehavior(), nc -> {
            // A silent responder on the whole JetStream API space: it receives the request but
            // never publishes a reply, so there is no 503 and the request must time out.
            Dispatcher d = nc.createDispatcher(msg -> { /* deliberately never reply */ });
            d.subscribe("$JS.API.>");

            JetStreamOptions jso = JetStreamOptions.builder()
                .requestTimeout(Duration.ofMillis(300))
                .build();
            JetStreamManagement jsm = nc.jetStreamManagement(jso);

            RequestFailureException e = assertThrows(RequestFailureException.class,
                () -> jsm.getStreamInfo("no-such-stream-anywhere"));

            assertEquals(RequestFailureReason.TIMEOUT, e.getReason());
            assertEquals(Connection.Status.CONNECTED, e.getConnectionStatus());
            assertTrue(e.getLastError() == null || e.getLastError().isEmpty());
        });
    }

    // ----------------------------------------------------------------------------------------------------
    // The feature also applies to core (non JetStream) requests: with the flag ON, a request that comes
    // back without a response returns a RequestFailureMessage carrier (which is a Message) instead of
    // null, and the carrier exposes the classified reason. With the flag OFF the legacy null is returned.
    // ----------------------------------------------------------------------------------------------------
    @Test
    public void testCoreRequestReturnsCarrierWhenFlagOn() throws Exception {
        runInServer(Options.builder().advancedRequestBehavior(), nc -> {
            Message m = nc.request("no.responder.subject", null, Duration.ofMillis(300));
            assertInstanceOf(RequestFailureMessage.class, m);
            RequestFailureMessage rfm = (RequestFailureMessage) m;
            assertEquals(RequestFailureReason.NO_RESPONDERS, rfm.getReason());
            assertEquals(Connection.Status.CONNECTED, rfm.getConnectionStatus());
        });
    }

    @Test
    public void testCoreRequestNullContractPreservedWhenFlagOff() throws Exception {
        // flag OFF (default): no responder -> the public request still returns null (legacy contract)
        runInServer(nc -> {
            Message m = nc.request("no.responder.subject", null, Duration.ofMillis(300));
            assertNull(m);
        });
    }

    // ----------------------------------------------------------------------------------------------------
    // CONNECTION_CLOSING - start a JetStream request whose responder never replies, then stop the server
    // out from under the in-flight request so the future is cancelled-closing (or the status drops to
    // RECONNECTING/DISCONNECTED). Both signals classify to CONNECTION_CLOSING.
    // This is inherently a bit racy; we use a generous request timeout and a separate thread to trigger
    // the request, then close the server, and assert on the resulting reason.
    // ----------------------------------------------------------------------------------------------------
    @Test
    public void testConnectionClosingReasonWhenFlagOn() throws Exception {
        NatsTestServer ts = new NatsTestServer(false);
        Options options = Options.builder()
            .server(ts.getURI())
            .advancedRequestBehavior()
            .maxReconnects(0) // do not try to reconnect; let the request fail fast on close
            .build();

        try (Connection nc = standardConnection(options)) {
            // silent responder so the request would otherwise sit waiting (no 503, no reply)
            Dispatcher d = nc.createDispatcher(msg -> { /* never reply */ });
            d.subscribe("$JS.API.>");

            JetStreamOptions jso = JetStreamOptions.builder()
                .requestTimeout(Duration.ofSeconds(10)) // long, so the close (not a timeout) ends it
                .build();
            JetStreamManagement jsm = nc.jetStreamManagement(jso);

            AtomicReference<Throwable> thrown = new AtomicReference<>();
            Thread requester = new Thread(() -> {
                try {
                    jsm.getStreamInfo("no-such-stream-anywhere");
                }
                catch (Throwable t) {
                    thrown.set(t);
                }
            });
            requester.start();

            // give the request a moment to be in-flight, then yank the server
            sleep(500);
            ts.close();

            requester.join(15000);

            Throwable t = thrown.get();
            assertNotNull(t, "the in-flight JetStream request should have failed when the server stopped");
            assertTrue(t instanceof RequestFailureException,
                "expected RequestFailureException but got " + t);
            RequestFailureException e = (RequestFailureException) t;
            assertEquals(RequestFailureReason.CONNECTION_CLOSING, e.getReason(),
                "expected CONNECTION_CLOSING, message was: " + e.getMessage());
        }
        finally {
            ts.close(); // idempotent; ensure cleanup even if assertions above threw
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // SERVER_ERROR is NOT covered here. To deterministically populate the connection's lastError with a
    // server -ERR at the exact moment a JetStream request fails, you need a server configured with
    // restricted permissions (e.g. a user without subscribe permission on its own inbox) so the inbox
    // subscription draws a permissions violation, and then time a JetStream request to observe a non-empty
    // getLastError(). That requires a custom server .conf with authorization blocks plus careful timing of
    // when the -ERR lands relative to the request future completing. It is too involved / timing-sensitive
    // to make reliable at this integration level, so it is intentionally omitted rather than flaky.
    //
    // CANCELLED has a dedicated branch (a cancellation that is not a 503 no-responders, not connection
    // closing, and not a cleanup-timeout), but it represents a genuine/unmodeled cancel that no normal
    // server scenario produces deterministically, so it is not covered here.
    // ----------------------------------------------------------------------------------------------------
}
