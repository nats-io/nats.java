// Copyright 2023 The NATS Authors
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
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.nats.client.support.JsonUtils.*;
import static io.nats.client.support.JsonValue.EMPTY_OBJECT;

/**
 * Internal json value helpers.
 */
public abstract class JsonValueUtils {
    public static final String Q = "\"";
    public static final char COMMA = ',';
    public static final String NULL_STR = "null";
    public static final String QUOTE = "\"";
    public static final String EMPTY_STRING = "";

    private JsonValueUtils() {} /* ensures cannot be constructed */

    public static String valueString(String s) {
        return Q + Encoding.jsonEncode(s) + Q;
    }

    public static String valueString(boolean b) {
        return Boolean.toString(b).toLowerCase();
    }

    public static String valueString(Map<String, JsonValue> map) {
        StringBuilder sbo = beginJson();
        for (String key : map.keySet()) {
            addField(sbo, key, map.get(key));
        }
        return endJson(sbo).toString();
    }

    public static String valueString(List<JsonValue> list) {
        StringBuilder sba = beginArray();
        for (JsonValue v : list) {
            sba.append(v.toJson());
            sba.append(COMMA);
        }
        return endArray(sba).toString();
    }

    public static String valueString(JsonValue[] array) {
        StringBuilder sba = beginArray();
        for (JsonValue v : array) {
            sba.append(v.toJson());
            sba.append(COMMA);
        }
        return endArray(sba).toString();
    }

    public static interface JsonValueSupplier<T> {
        T get(JsonValue v);
    }

    public static <T> T getMapped(JsonValue jsonValue, String key, JsonValueSupplier<T> valueSupplier) {
        JsonValue v = jsonValue == null || jsonValue.map == null ? null : jsonValue.map.get(key);
        return valueSupplier.get(v);
    }

    public static JsonValue getMappedValue(JsonValue jsonValue, String key) {
        return getMapped(jsonValue, key, v -> v);
    }

    public static JsonValue getMappedObjectOrEmpty(JsonValue jsonValue, String key) {
        return getMapped(jsonValue, key, v -> v == null ? EMPTY_OBJECT : v);
    }

    public static String getMappedString(JsonValue jsonValue, String key) {
        return getMapped(jsonValue, key, v -> v == null ? null : v.string);
    }

    public static String getMappedString(JsonValue jsonValue, String key, String dflt) {
        return getMapped(jsonValue, key, v -> v == null ? dflt : v.string);
    }

    public static List<String> getMappedStringList(JsonValue jsonValue, String key) {
        return getMapped(jsonValue, key, v -> {
            List<String> list = new ArrayList<>();
            if (v != null && v.array != null) {
                for (JsonValue jv : v.array) {
                    list.add(jv.asString());
                }
            }
            return list;
        });
    }

    public static ZonedDateTime getMappedDate(JsonValue jsonValue, String key) {
        return getMapped(jsonValue, key,
            v -> v == null || v.string == null ? null : DateTimeUtils.parseDateTimeThrowParseError(v.string));
    }

    public static Integer getMappedInteger(JsonValue jsonValue, String key) {
        return getMapped(jsonValue, key, v -> v == null ? null : v.asInteger());
    }

    public static int getMappedInteger(JsonValue jsonValue, String key, int dflt) {
        return getMapped(jsonValue, key, v -> v == null ? dflt : v.asInteger());
    }

    public static Long getMappedLong(JsonValue jsonValue, String key) {
        return getMapped(jsonValue, key, v -> v == null ? null : v.asLong());
    }

    public static long getMappedLong(JsonValue jsonValue, String key, long dflt) {
        return getMapped(jsonValue, key, v -> v == null ? dflt : v.asLong());
    }

    public static boolean getMappedBoolean(JsonValue jsonValue, String key) {
        return getMappedBoolean(jsonValue, key, false);
    }

    public static Boolean getMappedBoolean(JsonValue jsonValue, String key, Boolean dflt) {
        return getMapped(jsonValue, key,
            v -> v == null || v.bool == null ? dflt : v.bool);
    }

    public static Duration getMappedNanos(JsonValue jsonValue, String key) {
        Long l = getMappedLong(jsonValue, key);
        return l == null ? null : Duration.ofNanos(l);
    }

    public static Duration getMappedNanos(JsonValue jsonValue, String key, Duration dflt) {
        Long l = getMappedLong(jsonValue, key);
        return l == null ? dflt : Duration.ofNanos(l);
    }

    public static List<Duration> getMappedNanosList(JsonValue jsonValue, String key) {
        return getMapped(jsonValue, key, v -> {
            List<Duration> list = new ArrayList<>();
            if (v != null && v.array != null) {
                for (JsonValue jv : v.array) {
                    list.add(Duration.ofNanos(jv.asLong()));
                }
            }
            return list;
        });
    }
}
