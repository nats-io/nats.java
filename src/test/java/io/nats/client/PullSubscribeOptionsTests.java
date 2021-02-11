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
                .stream(STREAM)
                .durable(DURABLE)
                .build();
        assertEquals(STREAM, so.getStream());
        assertEquals(DURABLE, so.getDurable());

        assertNotNull(so.toString()); // COVERAGE
    }

    @Test
    public void testBuildValidation() {
        PullSubscribeOptions.Builder builder = PullSubscribeOptions.builder();
        // durable required direct or in configuration
        assertThrows(IllegalArgumentException.class, builder::build);

        final ConsumerConfiguration ccNoDur = ConsumerConfiguration.builder().build();
        assertThrows(IllegalArgumentException.class, () -> builder.configuration(ccNoDur).build());

        // durable directly
        builder.durable(DURABLE).build();

        // in configuration
        ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(DURABLE).build();
        PullSubscribeOptions.builder().configuration(cc).build();
    }


    @Test
    public void testFieldValidation() {
        PullSubscribeOptions.Builder builder = PullSubscribeOptions.builder();
        assertThrows(IllegalArgumentException.class, () -> builder.stream("foo.").build());
        assertThrows(IllegalArgumentException.class, () -> builder.durable("foo.").build());
    }
}
