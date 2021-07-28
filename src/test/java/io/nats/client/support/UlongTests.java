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

package io.nats.client.support;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

public final class UlongTests {

    @Test
    public void testUlong() {
        assertEquals(Ulong.ZERO, new Ulong("0"));
        assertEquals(Ulong.ONE, new Ulong("1"));
        assertEquals(Ulong.ZERO, new Ulong(0));
        assertEquals(Ulong.ONE, new Ulong(1));
        assertEquals(Ulong.MAX_VALUE, new Ulong("18446744073709551615"));

        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> new Ulong((String)null));
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> new Ulong((BigDecimal)null));

        assertThrows(NumberFormatException.class, () -> new Ulong("-1"));
        assertThrows(NumberFormatException.class, () -> new Ulong("2.1"));
        assertThrows(NumberFormatException.class, () -> new Ulong("18446744073709551616"));

        assertThrows(NumberFormatException.class, () -> new Ulong(new BigDecimal("-1")));
        assertThrows(NumberFormatException.class, () -> new Ulong(new BigDecimal("2.1")));
        assertThrows(NumberFormatException.class, () -> new Ulong(new BigDecimal("18446744073709551616")));

        assertEquals(0, Ulong.ZERO.value().longValue());
        assertEquals(1, Ulong.ONE.value().longValue());
        assertEquals(BigDecimal.ZERO, Ulong.ZERO.value());
        assertEquals(BigDecimal.ONE, Ulong.ONE.value());
        assertEquals(new BigDecimal("18446744073709551615"), Ulong.MAX_VALUE.value());

        assertEquals("18446744073709551615", Ulong.MAX_VALUE.toString());

        assertTrue(Ulong.ZERO.equalTo(new Ulong(0)));
        assertTrue(Ulong.ZERO.lessThan(Ulong.ONE));
        assertTrue(Ulong.ONE.greaterThan(Ulong.ZERO));
        assertFalse(Ulong.ZERO.equalTo(Ulong.ONE));
        assertFalse(Ulong.ONE.lessThan(Ulong.ZERO));
        assertFalse(Ulong.ZERO.greaterThan(Ulong.ONE));

        assertEquals(0, Ulong.ZERO.hashCode());
        assertEquals(Ulong.ZERO, Ulong.ZERO);
        assertEquals(new BigDecimal("18446744073709551615").hashCode(), new Ulong("18446744073709551615").hashCode());

        assertNotEquals(Ulong.ZERO, null);
        assertNotEquals(Ulong.ZERO, new Object());

        assertEquals(0, Ulong.ZERO.compareTo(new Ulong(0)));
        assertEquals(1, Ulong.ONE.compareTo(Ulong.ZERO));
        assertEquals(-1, Ulong.ZERO.compareTo(Ulong.ONE));
    }
}
