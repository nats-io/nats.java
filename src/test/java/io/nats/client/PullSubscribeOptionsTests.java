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

import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class PullSubscribeOptionsTests extends TestBase {

    @Test
    public void testAffirmative() {
        PullSubscribeOptions so = PullSubscribeOptions.builder()
                .defaultBatchSize(1)
                .defaultNoWait(true)
                .stream(STREAM)
                .durable(DURABLE)
                .build();
        assertEquals(1, so.getDefaultBatchSize());
        assertTrue(so.getDefaultNoWait());
        assertEquals(STREAM, so.getStream());
        assertEquals(DURABLE, so.getDurable());
    }

    @Test
    public void testBuildValidation() {
        PullSubscribeOptions.Builder builder = PullSubscribeOptions.builder();
        assertThrows(IllegalArgumentException.class, builder::build);
        builder.defaultBatchSize(1); // need batch size
        assertThrows(IllegalArgumentException.class, builder::build);
        builder.durable(DURABLE); // also need durable
        builder.build();
    }


    @Test
    public void testFieldValidation() {
        PullSubscribeOptions.Builder builder = PullSubscribeOptions.builder();
        assertThrows(IllegalArgumentException.class, () -> builder.defaultBatchSize(0));
        assertThrows(IllegalArgumentException.class, () -> builder.defaultBatchSize(-1));
        assertThrows(IllegalArgumentException.class, () -> builder.defaultBatchSize(257));
        assertThrows(IllegalArgumentException.class, () -> builder.stream("foo."));
        assertThrows(IllegalArgumentException.class, () -> builder.durable(null));
        assertThrows(IllegalArgumentException.class, () -> builder.durable("foo."));
    }
}