// Copyright 2020 The NATS Authors
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

import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import static io.nats.client.SubscribeOptions.DEFAULT_ORDERED_HEARTBEAT;
import static io.nats.client.support.NatsConstants.EMPTY;
import static io.nats.client.support.NatsJetStreamClientError.*;
import static org.junit.jupiter.api.Assertions.*;

public class SubscribeOptionsTests extends TestBase {
    private static final String[] badNames = {HAS_DOT, HAS_GT, HAS_STAR, HAS_FWD_SLASH, HAS_BACK_SLASH};

    @Test
    public void testPushAffirmative() {
        PushSubscribeOptions so = PushSubscribeOptions.builder().build();

        // starts out all null which is fine
        assertNull(so.getStream());
        assertNull(so.getDurable());
        assertNull(so.getDeliverSubject());

        so = PushSubscribeOptions.builder()
                .stream(STREAM).durable(DURABLE).deliverSubject(DELIVER).build();

        assertEquals(STREAM, so.getStream());
        assertEquals(DURABLE, so.getDurable());
        assertEquals(DELIVER, so.getDeliverSubject());

        // demonstrate that you can clear the builder
        so = PushSubscribeOptions.builder()
                .stream(null).deliverSubject(null).durable(null).build();
        assertNull(so.getStream());
        assertNull(so.getDurable());
        assertNull(so.getDeliverSubject());
        assertFalse(so.isPull());

        assertNotNull(so.toString()); // COVERAGE
    }

    @Test
    public void testDurableValidation() {
        // push
        assertNull(PushSubscribeOptions.builder()
                .durable(null)
                .configuration(ConsumerConfiguration.builder().durable(null).build())
                .build()
                .getDurable());

        assertEquals("y", PushSubscribeOptions.builder()
                .durable(null)
                .configuration(ConsumerConfiguration.builder().durable("y").build())
                .build()
                .getDurable());

        assertEquals("x", PushSubscribeOptions.builder()
                .durable("x")
                .configuration(ConsumerConfiguration.builder().durable(null).build())
                .build()
                .getDurable());

        assertEquals("x", PushSubscribeOptions.builder()
                .durable("x")
                .configuration(ConsumerConfiguration.builder().durable("x").build())
                .build()
                .getDurable());

        assertClientError(JsSoDurableMismatch, () -> PushSubscribeOptions.builder()
                .durable("x")
                .configuration(ConsumerConfiguration.builder().durable("y").build())
                .build());

        assertNull(PushSubscribeOptions.builder().build().getDurable());

        // pull
        assertEquals("y", PullSubscribeOptions.builder()
                .durable(null)
                .configuration(ConsumerConfiguration.builder().durable("y").build())
                .build()
                .getDurable());

        assertEquals("x", PullSubscribeOptions.builder()
                .durable("x")
                .configuration(ConsumerConfiguration.builder().durable(null).build())
                .build()
                .getDurable());

        assertEquals("x", PullSubscribeOptions.builder()
                .durable("x")
                .configuration(ConsumerConfiguration.builder().durable("x").build())
                .build()
                .getDurable());

        assertClientError(JsSoDurableMismatch, () -> PullSubscribeOptions.builder()
                .durable("x")
                .configuration(ConsumerConfiguration.builder().durable("y").build())
                .build());
    }

    @Test
    public void testDeliverGroupValidation() {
        assertNull(PushSubscribeOptions.builder()
                .deliverGroup(null)
                .configuration(ConsumerConfiguration.builder().deliverGroup(null).build())
                .build()
                .getDeliverGroup());

        assertEquals("y", PushSubscribeOptions.builder()
                .deliverGroup(null)
                .configuration(ConsumerConfiguration.builder().deliverGroup("y").build())
                .build()
                .getDeliverGroup());

        assertEquals("x", PushSubscribeOptions.builder()
                .deliverGroup("x")
                .configuration(ConsumerConfiguration.builder().deliverGroup(null).build())
                .build()
                .getDeliverGroup());

        assertEquals("x", PushSubscribeOptions.builder()
                .deliverGroup("x")
                .configuration(ConsumerConfiguration.builder().deliverGroup("x").build())
                .build()
                .getDeliverGroup());

        assertClientError(JsSoDeliverGroupMismatch, () -> PushSubscribeOptions.builder()
                .deliverGroup("x")
                .configuration(ConsumerConfiguration.builder().deliverGroup("y").build())
                .build());
    }

    @Test
    public void testDeliverSubjectValidation() {
        assertNull(PushSubscribeOptions.builder()
                .deliverSubject(null)
                .configuration(ConsumerConfiguration.builder().deliverSubject(null).build())
                .build()
                .getDeliverSubject());

        assertEquals("y", PushSubscribeOptions.builder()
                .deliverSubject(null)
                .configuration(ConsumerConfiguration.builder().deliverSubject("y").build())
                .build()
                .getDeliverSubject());

        assertEquals("x", PushSubscribeOptions.builder()
                .deliverSubject("x")
                .configuration(ConsumerConfiguration.builder().deliverSubject(null).build())
                .build()
                .getDeliverSubject());

        assertEquals("x", PushSubscribeOptions.builder()
                .deliverSubject("x")
                .configuration(ConsumerConfiguration.builder().deliverSubject("x").build())
                .build()
                .getDeliverSubject());

        assertClientError(JsSoDeliverSubjectMismatch, () -> PushSubscribeOptions.builder()
                .deliverSubject("x")
                .configuration(ConsumerConfiguration.builder().deliverSubject("y").build())
                .build());
    }

    @Test
    public void testPullAffirmative() {
        PullSubscribeOptions.Builder builder = PullSubscribeOptions.builder()
                .stream(STREAM)
                .durable(DURABLE);

        PullSubscribeOptions so = builder.build();
        assertEquals(STREAM, so.getStream());
        assertEquals(DURABLE, so.getDurable());
        assertTrue(so.isPull());

        assertNotNull(so.toString()); // COVERAGE
    }

    @Test
    public void testPushFieldValidation() {
        for (String bad : badNames) {
            PushSubscribeOptions.Builder pushBuilder = PushSubscribeOptions.builder();
            assertThrows(IllegalArgumentException.class, () -> pushBuilder.stream(bad).build());
            assertThrows(IllegalArgumentException.class, () -> pushBuilder.durable(bad).build());
            assertThrows(IllegalArgumentException.class, () -> pushBuilder.name(bad).build());
            assertThrows(IllegalArgumentException.class, () -> ConsumerConfiguration.builder().durable(bad).build());
            assertThrows(IllegalArgumentException.class, () -> ConsumerConfiguration.builder().name(bad).build());
        }

        assertClientError(JsConsumerNameDurableMismatch, () -> PushSubscribeOptions.builder().name(NAME).durable(DURABLE).build());

        // durable directly
        PushSubscribeOptions.builder().durable(DURABLE).build();
        PushSubscribeOptions.builder().name(NAME).build();

        // in configuration
        ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(DURABLE).build();
        PushSubscribeOptions.builder().configuration(cc).build();
        cc = ConsumerConfiguration.builder().name(NAME).build();
        PushSubscribeOptions.builder().configuration(cc).build();

        // new helper
        ConsumerConfiguration.builder().durable(DURABLE).buildPushSubscribeOptions();
        ConsumerConfiguration.builder().name(NAME).buildPushSubscribeOptions();
    }

    @Test
    public void testPullFieldValidation() {
        for (String bad : badNames) {
            PullSubscribeOptions.Builder pullBuilder = PullSubscribeOptions.builder();
            assertThrows(IllegalArgumentException.class, () -> pullBuilder.stream(bad).build());
            assertThrows(IllegalArgumentException.class, () -> pullBuilder.durable(bad).build());
            assertThrows(IllegalArgumentException.class, () -> pullBuilder.name(bad).build());
            assertThrows(IllegalArgumentException.class, () -> ConsumerConfiguration.builder().durable(bad).build());
            assertThrows(IllegalArgumentException.class, () -> ConsumerConfiguration.builder().name(bad).build());
        }

        assertClientError(JsConsumerNameDurableMismatch, () -> PullSubscribeOptions.builder().name(NAME).durable(DURABLE).build());

        // durable directly
        PullSubscribeOptions.builder().durable(DURABLE).build();

        // in configuration
        ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(DURABLE).build();
        PullSubscribeOptions.builder().configuration(cc).build();

        // new helper
        ConsumerConfiguration.builder().durable(DURABLE).buildPullSubscribeOptions();
    }

    @Test
    public void testCreationErrors() {
        ConsumerConfiguration cc1 = ConsumerConfiguration.builder().durable(durable((1))).build();
        assertClientError(JsSoDurableMismatch,
            () -> PushSubscribeOptions.builder().configuration(cc1).durable(durable(2)).build());

        ConsumerConfiguration cc2 = ConsumerConfiguration.builder().deliverGroup(deliver((1))).build();
        assertClientError(JsSoDeliverGroupMismatch,
            () -> PushSubscribeOptions.builder().configuration(cc2).deliverGroup(deliver(2)).build());

        ConsumerConfiguration cc3 = ConsumerConfiguration.builder().name(name((1))).build();
        assertClientError(JsSoNameMismatch,
            () -> PushSubscribeOptions.builder().configuration(cc3).name(name(2)).build());
    }

    @Test
    public void testBindCreationErrors() {
        String durOrName = name();

        // bind
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.bind(null, durOrName));
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.bind(EMPTY, durOrName));
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.bind(STREAM, null));
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.bind(STREAM, EMPTY));
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().stream(STREAM).bind(true).build());

        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().stream(EMPTY).durable(durOrName).bind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().durable(durOrName).bind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().stream(STREAM).durable(EMPTY).bind(true).build());

        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().stream(EMPTY).name(durOrName).bind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().name(durOrName).bind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().stream(STREAM).name(EMPTY).bind(true).build());

        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.bind(null, durOrName));
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.bind(EMPTY, durOrName));
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.bind(STREAM, null));
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.bind(STREAM, EMPTY));
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().stream(STREAM).bind(true).build());

        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().stream(EMPTY).durable(durOrName).bind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().durable(durOrName).bind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().stream(STREAM).durable(EMPTY).bind(true).build());

        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().stream(EMPTY).name(durOrName).bind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().name(durOrName).bind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().stream(STREAM).name(EMPTY).bind(true).build());

        // fast bind
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.fastBind(null, durOrName));
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.fastBind(EMPTY, durOrName));
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.fastBind(STREAM, null));
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.fastBind(STREAM, EMPTY));
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().stream(STREAM).fastBind(true).build());

        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().stream(EMPTY).durable(durOrName).fastBind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().durable(durOrName).fastBind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().stream(STREAM).durable(EMPTY).fastBind(true).build());

        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().stream(EMPTY).name(durOrName).fastBind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().name(durOrName).fastBind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().stream(STREAM).name(EMPTY).fastBind(true).build());

        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.fastBind(null, durOrName));
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.fastBind(EMPTY, durOrName));
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.fastBind(STREAM, null));
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.fastBind(STREAM, EMPTY));
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().stream(STREAM).fastBind(true).build());

        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().stream(EMPTY).durable(durOrName).fastBind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().durable(durOrName).fastBind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().stream(STREAM).durable(EMPTY).fastBind(true).build());

        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().stream(EMPTY).name(durOrName).fastBind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().name(durOrName).fastBind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().stream(STREAM).name(EMPTY).fastBind(true).build());
    }

    @Test
    public void testOrderedCreation() {
        ConsumerConfiguration ccEmpty = ConsumerConfiguration.builder().build();
        PushSubscribeOptions.builder().configuration(ccEmpty).ordered(true).build();

        assertClientError(JsSoOrderedNotAllowedWithBind,
            () -> PushSubscribeOptions.builder().stream(STREAM).bind(true).ordered(true).build());

        assertClientError(JsSoOrderedNotAllowedWithDeliverGroup,
            () -> PushSubscribeOptions.builder().deliverGroup(DELIVER).ordered(true).build());

        assertClientError(JsSoOrderedNotAllowedWithDurable,
            () -> PushSubscribeOptions.builder().durable(DURABLE).ordered(true).build());

        assertClientError(JsSoOrderedNotAllowedWithDeliverSubject,
            () -> PushSubscribeOptions.builder().deliverSubject(DELIVER).ordered(true).build());

        ConsumerConfiguration ccAckNotNone1 = ConsumerConfiguration.builder().ackPolicy(AckPolicy.All).build();
        assertClientError(JsSoOrderedRequiresAckPolicyNone,
            () -> PushSubscribeOptions.builder().configuration(ccAckNotNone1).ordered(true).build());

        ConsumerConfiguration ccAckNotNone2 = ConsumerConfiguration.builder().ackPolicy(AckPolicy.Explicit).build();
        assertClientError(JsSoOrderedRequiresAckPolicyNone,
            () -> PushSubscribeOptions.builder().configuration(ccAckNotNone2).ordered(true).build());

        ConsumerConfiguration ccAckNoneOk = ConsumerConfiguration.builder().ackPolicy(AckPolicy.None).build();
        PushSubscribeOptions.builder().configuration(ccAckNoneOk).ordered(true).build();

        ConsumerConfiguration ccMax = ConsumerConfiguration.builder().maxDeliver(2).build();
        assertClientError(JsSoOrderedRequiresMaxDeliverOfOne,
            () -> PushSubscribeOptions.builder().configuration(ccMax).ordered(true).build());

        ConsumerConfiguration ccHb = ConsumerConfiguration.builder().idleHeartbeat(100).build();
        PushSubscribeOptions pso = PushSubscribeOptions.builder().configuration(ccHb).ordered(true).build();
        assertEquals(100, pso.getConsumerConfiguration().getIdleHeartbeat().toMillis());

        ccHb = ConsumerConfiguration.builder().idleHeartbeat(DEFAULT_ORDERED_HEARTBEAT + 1).build();
        pso = PushSubscribeOptions.builder().configuration(ccHb).ordered(true).build();
        assertEquals(DEFAULT_ORDERED_HEARTBEAT + 1, pso.getConsumerConfiguration().getIdleHeartbeat().toMillis());

        // okay if you set it to true
        ConsumerConfiguration cc = ConsumerConfiguration.builder().memStorage(true).build();
        PushSubscribeOptions.builder().configuration(cc).ordered(true).build();

        // okay if you set it to 1
        cc = ConsumerConfiguration.builder().numReplicas(1).build();
        PushSubscribeOptions.builder().configuration(cc).ordered(true).build();

        // not okay if you set it to false
        ConsumerConfiguration ccMs = ConsumerConfiguration.builder().memStorage(false).build();
        assertClientError(JsSoOrderedMemStorageNotSuppliedOrTrue,
            () -> PushSubscribeOptions.builder().configuration(ccMs).ordered(true).build());

        // not okay if you set it to something other than 1
        ConsumerConfiguration ccR = ConsumerConfiguration.builder().numReplicas(3).build();
        assertClientError(JsSoOrderedReplicasNotSuppliedOrOne,
            () -> PushSubscribeOptions.builder().configuration(ccR).ordered(true).build());
    }
}
