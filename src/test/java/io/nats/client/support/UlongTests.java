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

        Ulong incdec = new Ulong(0);
        incdec.increment();
        assertEquals(Ulong.ONE, incdec);
        incdec.decrement();
        assertEquals(Ulong.ZERO, incdec);
        incdec.decrement();
        assertEquals(Ulong.MAX_VALUE, incdec);
        incdec.increment();
        assertEquals(Ulong.ZERO, incdec);

        assertThrows(IllegalStateException.class, Ulong.ZERO::increment);
        assertThrows(IllegalStateException.class, Ulong.ONE::increment);
        assertThrows(IllegalStateException.class, Ulong.MAX_VALUE::increment);
        assertThrows(IllegalStateException.class, Ulong.ZERO::decrement);
        assertThrows(IllegalStateException.class, Ulong.ONE::decrement);
        assertThrows(IllegalStateException.class, Ulong.MAX_VALUE::decrement);

        assertEquals(Ulong.ZERO, Ulong.ONE.add(Ulong.MAX_VALUE));
        assertEquals(Ulong.ZERO, Ulong.MAX_VALUE.add(Ulong.ONE));
        assertEquals(Ulong.ONE, Ulong.MAX_VALUE.add(new Ulong(2)));
        assertEquals(Ulong.MAX_VALUE, Ulong.ZERO.add(Ulong.MAX_VALUE));

        assertEquals(Ulong.ONE, Ulong.ZERO.subtract(Ulong.MAX_VALUE));
        assertEquals(new Ulong(2), Ulong.ONE.subtract(Ulong.MAX_VALUE));
        assertEquals(new Ulong(3), new Ulong(2).subtract(Ulong.MAX_VALUE));
        assertEquals(Ulong.MAX_VALUE, Ulong.ZERO.subtract(Ulong.ONE));
        assertEquals(Ulong.MAX_VALUE, Ulong.ONE.subtract(new Ulong(2)));
        assertEquals(Ulong.MAX_VALUE, new Ulong(2).subtract(new Ulong(3)));

        assertEquals(Ulong.MAX_VALUE.subtract(Ulong.ONE), Ulong.ZERO.subtract(new Ulong(2)));

        assertEquals(Ulong.ZERO, Ulong.ONE.subtract(Ulong.ONE));
        assertEquals(Ulong.ONE, new Ulong(2).subtract(Ulong.ONE));
        assertEquals(new Ulong(2), new Ulong(2).subtract(Ulong.ZERO));
        assertEquals(new Ulong(2), new Ulong(3).subtract(Ulong.ONE));
        assertEquals(Ulong.MAX_VALUE.subtract(Ulong.ONE), Ulong.MAX_VALUE.subtract(new Ulong(1)));
        assertEquals(Ulong.MAX_VALUE.subtract(new Ulong(2)), Ulong.MAX_VALUE.subtract(new Ulong(2)));
    }
}
