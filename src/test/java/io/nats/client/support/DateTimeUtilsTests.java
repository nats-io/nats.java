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

import io.nats.client.impl.DateTimeUtils;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class DateTimeUtilsTests {

    @Test
    public void testParseDateTime() {
        assertEquals(1611186068, DateTimeUtils.parseDateTime("2021-01-20T23:41:08.579594Z").toEpochSecond());
        assertEquals(1612293508, DateTimeUtils.parseDateTime("2021-02-02T11:18:28.347722551-08:00").toEpochSecond());
        assertEquals(DateTimeUtils.DEFAULT_TIME, DateTimeUtils.parseDateTime("anything-not-valid"));

        ZonedDateTime zdt1 = DateTimeUtils.parseDateTime("2021-01-20T18:41:08-05:00");
        ZonedDateTime zdt2 = DateTimeUtils.parseDateTime("2021-01-20T23:41:08.000000Z");
        assertEquals(zdt1, zdt2);

        zdt1 = ZonedDateTime.of(2012, 1, 12, 6, 30, 1, 500, DateTimeUtils.ZONE_ID_GMT);
        assertEquals(zdt1.toEpochSecond(), DateTimeUtils.parseDateTime("2012-01-12T06:30:01.000000500Z").toEpochSecond());
    }

    @Test
    public void testToRfc3339() {
        Instant i = Instant.ofEpochSecond(1611186068);
        ZonedDateTime zdt1 = ZonedDateTime.ofInstant(i, ZoneId.systemDefault());
        ZonedDateTime zdt2 = ZonedDateTime.ofInstant(i, DateTimeUtils.ZONE_ID_GMT);
        System.out.println(zdt1);
        System.out.println(zdt2);
        assertEquals(zdt1.toEpochSecond(), zdt2.toEpochSecond());

        String rfc1 = DateTimeUtils.toRfc3339(zdt1);
        String rfc2 = DateTimeUtils.toRfc3339(zdt2);
        assertEquals(rfc1, rfc2);
        System.out.println(zdt2.toEpochSecond());

        assertEquals("2021-01-20T23:41:08.579594000Z", DateTimeUtils.toRfc3339(DateTimeUtils.parseDateTime("2021-01-20T23:41:08.579594Z")));
        assertEquals("2021-02-02T19:18:28.347722551Z", DateTimeUtils.toRfc3339(DateTimeUtils.parseDateTime("2021-02-02T11:18:28.347722551-08:00")));
    }

    @Test
    public void testFromNow() {
        long now = Instant.now().toEpochMilli();
        long then = Instant.from(DateTimeUtils.fromNow(5000)).toEpochMilli();
        assertTrue(then - now < 5050); // it takes about 10 ms to execute fromNow

        now = Instant.now().toEpochMilli();
        then = Instant.from(DateTimeUtils.fromNow(Duration.ofMillis(5000))).toEpochMilli();
        assertTrue(then - now < 5050);
    }
}
