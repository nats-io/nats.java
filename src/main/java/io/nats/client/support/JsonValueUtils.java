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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.function.Function;

import static io.nats.client.support.JsonValue.EMPTY_ARRAY;
import static io.nats.client.support.JsonValue.EMPTY_OBJECT;

/**
 * Internal json value helpers.
 */
public abstract class JsonValueUtils {

    private JsonValueUtils() {} /* ensures cannot be constructed */

    public interface JsonValueSupplier<T> {
        T get(JsonValue v);
    }

    public static <T> T read(JsonValue jsonValue, String key, JsonValueSupplier<T> valueSupplier) {
        JsonValue v = jsonValue == null || jsonValue.map == null ? null : jsonValue.map.get(key);
        return valueSupplier.get(v);
    }

    public static JsonValue readValue(JsonValue jsonValue, String key) {
        return read(jsonValue, key, v -> v);
    }

    public static JsonValue readObject(JsonValue jsonValue, String key) {
        return read(jsonValue, key, v -> v == null ? EMPTY_OBJECT : v);
    }

    public static List<JsonValue> readArray(JsonValue jsonValue, String key) {
        return read(jsonValue, key, v -> v == null ? EMPTY_ARRAY.array : v.array);
    }

    public static String readString(JsonValue jsonValue, String key) {
        return read(jsonValue, key, v -> v == null ? null : v.string);
    }

    public static String readString(JsonValue jsonValue, String key, String dflt) {
        return read(jsonValue, key, v -> v == null ? dflt : v.string);
    }

    public static ZonedDateTime readDate(JsonValue jsonValue, String key) {
        return read(jsonValue, key,
            v -> v == null || v.string == null ? null : DateTimeUtils.parseDateTimeThrowParseError(v.string));
    }

    public static Integer readInteger(JsonValue jsonValue, String key) {
        return read(jsonValue, key, v -> v == null ? null : getInteger(v));
    }

    public static int readInteger(JsonValue jsonValue, String key, int dflt) {
        return read(jsonValue, key, v -> {
            if (v != null) {
                Integer i = getInteger(v);
                if (i != null) {
                    return i;
                }
            }
            return dflt;
        });
    }

    public static Long readLong(JsonValue jsonValue, String key) {
        return read(jsonValue, key, v -> v == null ? null : getLong(v));
    }

    public static long readLong(JsonValue jsonValue, String key, long dflt) {
        return read(jsonValue, key, v -> {
            if (v != null) {
                Long l = getLong(v);
                if (l != null) {
                    return l;
                }
            }
            return dflt;
        });
    }

    public static boolean readBoolean(JsonValue jsonValue, String key) {
        return readBoolean(jsonValue, key, false);
    }

    public static Boolean readBoolean(JsonValue jsonValue, String key, Boolean dflt) {
        return read(jsonValue, key,
            v -> v == null || v.bool == null ? dflt : v.bool);
    }

    public static Duration readNanos(JsonValue jsonValue, String key) {
        Long l = readLong(jsonValue, key);
        return l == null ? null : Duration.ofNanos(l);
    }

    public static Duration readNanos(JsonValue jsonValue, String key, Duration dflt) {
        Long l = readLong(jsonValue, key);
        return l == null ? dflt : Duration.ofNanos(l);
    }

    public static <T> List<T> listOf(JsonValue v, Function<JsonValue, T> provider) {
        List<T> list = new ArrayList<>();
        if (v != null && v.array != null) {
            for (JsonValue jv : v.array) {
                T t = provider.apply(jv);
                if (t != null) {
                    list.add(t);
                }
            }
        }
        return list;
    }

    public static <T> List<T> optionalListOf(JsonValue v, Function<JsonValue, T> provider) {
        List<T> list = listOf(v, provider);
        return list.size() == 0 ? null : list;
    }

    public static List<String> readStringList(JsonValue jsonValue, String key) {
        return read(jsonValue, key, v -> listOf(v, jv -> jv.string));
    }

    public static List<String> readStringListIgnoreEmpty(JsonValue jsonValue, String key) {
        return read(jsonValue, key, v -> listOf(v, jv -> {
            if (jv.string != null) {
                String s = jv.string.trim();
                if (s.length() > 0) {
                    return s;
                }
            }
            return null;
        }));
    }

    public static List<String> readOptionalStringList(JsonValue jsonValue, String key) {
        return read(jsonValue, key, v -> optionalListOf(v, jv -> jv.string));
    }

    public static List<Long> readLongList(JsonValue jsonValue, String key) {
        return read(jsonValue, key, v -> listOf(v, JsonValueUtils::getLong));
    }

    public static List<Duration> readNanosList(JsonValue jsonValue, String key) {
        return read(jsonValue, key,
            v -> listOf(v, vv -> {
                Long l = getLong(vv);
                return l == null ? null : Duration.ofNanos(l);
            })
        );
    }

    public static byte[] readBytes(JsonValue jsonValue, String key) {
        String s = readString(jsonValue, key);
        return s == null ? null : s.getBytes(StandardCharsets.US_ASCII);
    }

    public static byte[] readBase64(JsonValue jsonValue, String key) {
        String b64 = readString(jsonValue, key);
        return b64 == null ? null : Base64.getDecoder().decode(b64);
    }

    public static Integer getInteger(JsonValue v) {
        if (v.i != null) {
            return v.i;
        }
        // just in case the number was stored as a long, which is unlikely, but I want to handle it
        if (v.l != null && v.l <= Integer.MAX_VALUE && v.l >= Integer.MIN_VALUE) {
            return v.l.intValue();
        }
        return null;
    }

    public static Long getLong(JsonValue v) {
        return v.l != null ? v.l : (v.i != null ? (long)v.i : null);
    }

    public static long getLong(JsonValue v, long dflt) {
        return v.l != null ? v.l : (v.i != null ? (long)v.i : dflt);
    }

    public static JsonValue instance(Duration d) {
        return new JsonValue(d.toNanos());
    }
}