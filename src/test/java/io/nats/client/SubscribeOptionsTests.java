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

import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import static io.nats.client.support.NatsConstants.EMPTY;
import static org.junit.jupiter.api.Assertions.*;

public class SubscribeOptionsTests extends TestBase {

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

        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder()
                .durable("x")
                .configuration(ConsumerConfiguration.builder().durable("y").build())
                .build());

        assertNull(PushSubscribeOptions.builder().build().getDurable());

        // pull
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder()
                .durable(null)
                .configuration(ConsumerConfiguration.builder().durable(null).build())
                .build()
                .getDurable());

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

        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder()
                .durable("x")
                .configuration(ConsumerConfiguration.builder().durable("y").build())
                .build());

        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().build());
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

        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder()
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

        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder()
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
        PushSubscribeOptions.Builder builder = PushSubscribeOptions.builder();
        assertThrows(IllegalArgumentException.class, () -> builder.stream(HAS_DOT).build());
        assertThrows(IllegalArgumentException.class, () -> builder.durable(HAS_DOT).build());

        ConsumerConfiguration ccBadDur = ConsumerConfiguration.builder().durable(HAS_DOT).build();
        assertThrows(IllegalArgumentException.class, () -> builder.configuration(ccBadDur).build());

        // durable directly
        PushSubscribeOptions.builder().durable(DURABLE).build();

        // in configuration
        ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(DURABLE).build();
        PushSubscribeOptions.builder().configuration(cc).build();

        // new helper
        ConsumerConfiguration.builder().durable(DURABLE).buildPushSubscribeOptions();
    }

    @Test
    public void testPullValidation() {
        PullSubscribeOptions.Builder builder1 = PullSubscribeOptions.builder();
        assertThrows(IllegalArgumentException.class, () -> builder1.stream(HAS_DOT).build());
        assertThrows(IllegalArgumentException.class, () -> builder1.durable(HAS_DOT).build());

        ConsumerConfiguration ccBadDur = ConsumerConfiguration.builder().durable(HAS_DOT).build();
        assertThrows(IllegalArgumentException.class, () -> builder1.configuration(ccBadDur).build());

        // durable required direct or in configuration
        PullSubscribeOptions.Builder builder2 = PullSubscribeOptions.builder();
        assertThrows(IllegalArgumentException.class, builder2::build);

        ConsumerConfiguration ccNoDur = ConsumerConfiguration.builder().build();
        assertThrows(IllegalArgumentException.class, () -> builder2.configuration(ccNoDur).build());

        // durable directly
        PullSubscribeOptions.builder().durable(DURABLE).build();

        // in configuration
        ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(DURABLE).build();
        PullSubscribeOptions.builder().configuration(cc).build();

        // new helper
        ConsumerConfiguration.builder().durable(DURABLE).buildPullSubscribeOptions();
    }

    @Test
    public void testBindCreationErrors() {
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.bind(null, DURABLE));
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.bind(EMPTY, DURABLE));
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.bind(STREAM, null));
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.bind(STREAM, EMPTY));
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().stream(STREAM).bind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().stream(EMPTY).durable(DURABLE).bind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().durable(DURABLE).bind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().stream(STREAM).durable(EMPTY).bind(true).build());

        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.bind(null, DURABLE));
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.bind(EMPTY, DURABLE));
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.bind(STREAM, null));
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.bind(STREAM, EMPTY));
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().stream(STREAM).bind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().stream(EMPTY).durable(DURABLE).bind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().durable(DURABLE).bind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().stream(STREAM).durable(EMPTY).bind(true).build());
    }
}
