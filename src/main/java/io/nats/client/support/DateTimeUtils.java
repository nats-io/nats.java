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

package io.nats.client.support;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Internal json parsing helpers.
 */
public abstract class DateTimeUtils {
    private DateTimeUtils() {}  /* ensures cannot be constructed */

    public static final ZoneId ZONE_ID_GMT = ZoneId.of("GMT");
    public static final ZonedDateTime DEFAULT_TIME = ZonedDateTime.of(1, 1, 1, 0, 0, 0, 0, ZONE_ID_GMT);
    public static final DateTimeFormatter RFC3339_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.nnnnnnnnn'Z'");

    public static ZonedDateTime toGmt(ZonedDateTime zonedDateTime) {
        return ZonedDateTime.ofInstant(Instant.from(zonedDateTime), ZONE_ID_GMT);
    }

    public static String toRfc3339(ZonedDateTime zonedDateTime) {
        return RFC3339_FORMATTER.format(toGmt(zonedDateTime));
    }

    /**
     * Parses a date time from the server.
     * @param dateTime - date time from the server.
     * @return a Zoned Date time.
     */
    public static ZonedDateTime parseDateTime(String dateTime) {
        try {
            return toGmt(ZonedDateTime.parse(dateTime));
        }
        catch (DateTimeParseException s) {
            return DEFAULT_TIME;
        }
    }

    public static ZonedDateTime fromNow(long millis) {
        return ZonedDateTime.ofInstant(Instant.now().plusMillis(millis), ZONE_ID_GMT);
    }

    public static ZonedDateTime fromNow(Duration dur) {
        return ZonedDateTime.ofInstant(Instant.now().plusMillis(dur.toMillis()), ZONE_ID_GMT);
    }
}
