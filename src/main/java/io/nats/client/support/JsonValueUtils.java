// Copyright 2023-2025 The NATS Authors
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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.nats.client.support.Encoding.base64BasicDecode;
import static io.nats.client.support.JsonValue.Type;

/**
 * Internal json value helpers.
 */
public abstract class JsonValueUtils {
    private JsonValueUtils() {} /* ensures cannot be constructed */

    public static @Nullable JsonValue readValue(@Nullable JsonValue jsonValue, String key) {
        return jsonValue == null || jsonValue.map == null ? null
                : jsonValue.map.get(key);
    }

    public static JsonValue readObject(JsonValue jsonValue, String key) {
        JsonValue v = readValue(jsonValue,key);
        return v == null ? JsonValue.EMPTY_MAP : v;
    }

    public static List<JsonValue> readArray(JsonValue jsonValue, String key) {
        JsonValue v = readValue(jsonValue,key);
        return v == null ? JsonValue.EMPTY_ARRAY.array : v.array;
    }

    public static @Nullable Map<String, String> readStringStringMap(JsonValue jv, String key) {
        JsonValue o = readObject(jv, key);
        if (o.type == Type.MAP && o.map.size() > 0) {
            Map<String, String> temp = new HashMap<>(o.map.size() * 4/3 + 1);
            for (String k : o.map.keySet()) {
                String value = readString(o, k);
                if (value != null) {
                    temp.put(k, value);
                }
            }
            return temp.isEmpty() ? null : temp;
        }
        return null;
    }

    public static @Nullable String readString(@Nullable JsonValue jsonValue, String key) {
        JsonValue v = readValue(jsonValue,key);
        return v == null ? null : v.string;// todo number can be converted to string
    }

    public static @Nullable String readStringEmptyAsNull(JsonValue jsonValue, String key) {
        JsonValue v = readValue(jsonValue,key);
        return v == null || v.string == null || v.string.isEmpty() ? null
                : v.string;
    }

    public static String readString(JsonValue jsonValue, String key, String dflt) {
        JsonValue v = readValue(jsonValue,key);
        return v == null ? dflt : v.string;
    }

    public static @Nullable ZonedDateTime readDate(JsonValue jsonValue, String key) {
        String v = readString(jsonValue,key);
        return v == null ? null
                : DateTimeUtils.parseDateTimeThrowParseError(v);
    }

    public static @Nullable Integer readInteger(JsonValue jsonValue, String key) {
        JsonValue v = readValue(jsonValue,key);
        return v == null ? null : getInteger(v);
    }

    public static int readInteger(JsonValue jsonValue, String key, int dflt) {
        JsonValue v = readValue(jsonValue,key);
        if (v != null) {
            Integer i = getInteger(v);
            if (i != null) {
                return i;
            }
        }
        return dflt;
    }

    public static @Nullable Long readLong(JsonValue jsonValue, String key) {
        JsonValue v = readValue(jsonValue,key);
        return v == null ? null : getLong(v);
    }

    public static long readLong(JsonValue jsonValue, String key, long dflt) {
        JsonValue v = readValue(jsonValue,key);
        if (v != null) {
            Long l = getLong(v);
            if (l != null) {
                return l;
            }
        }
        return dflt;
    }

    public static boolean readBoolean(JsonValue jsonValue, String key) {
        return readBoolean(jsonValue, key, false);
    }

    public static Boolean readBoolean(JsonValue jsonValue, String key, Boolean dflt) {
        JsonValue v = readValue(jsonValue,key);
        return v == null || v.bool == null ? dflt : v.bool;
    }

    public static @Nullable Duration readNanos(JsonValue jsonValue, String key) {
        Long l = readLong(jsonValue, key);
        return l == null ? null : Duration.ofNanos(l);
    }

    public static Duration readNanos(JsonValue jsonValue, String key, Duration dflt) {
        Long l = readLong(jsonValue, key);
        return l == null ? dflt : Duration.ofNanos(l);
    }

    public static <T> List<T> listOf(@Nullable JsonValue v, Function<JsonValue, T> provider) {
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

    public static <T> @Nullable List<T> optionalListOf(@Nullable JsonValue v, Function<JsonValue, T> provider) {
        List<T> list = listOf(v, provider);
        return list.isEmpty() ? null : list;
    }

    public static List<String> readStringList(JsonValue jsonValue, String key) {
        JsonValue v = readValue(jsonValue,key);
        return listOf(v, jv -> jv.string);
    }

    public static List<String> readStringListIgnoreEmpty(JsonValue jsonValue, String key) {
        JsonValue v = readValue(jsonValue,key);
        return listOf(v, jv -> {
            if (jv.string != null) {
                String s = jv.string.trim();
                if (!s.isEmpty()) {
                    return s;
                }
            }
            return null;
        });
    }

    public static @Nullable List<String> readOptionalStringList(JsonValue jsonValue, String key) {
        JsonValue v = readValue(jsonValue,key);
        return optionalListOf(v, jv -> jv.string);
    }

    public static List<Long> readLongList(JsonValue jsonValue, String key) {
        JsonValue v = readValue(jsonValue,key);
        return listOf(v, JsonValueUtils::getLong);
    }
    public static @Nullable List<Duration> readNanosList(JsonValue jsonValue, String key) {
        return readNanosList(jsonValue, key, false);
    }

    public static @Nullable List<Duration> readNanosList(JsonValue jsonValue, String key, boolean nullIfEmpty) {
        JsonValue v = readValue(jsonValue,key);
        List<Duration> list = listOf(v, vv -> {
            Long l = getLong(vv);
            return l == null ? null : Duration.ofNanos(l);
        });
        return list.isEmpty() && nullIfEmpty ? null : list;
    }

    public static byte @Nullable [] readBytes(JsonValue jsonValue, String key) {
        String s = readString(jsonValue, key);
        return s == null ? null : s.getBytes(StandardCharsets.UTF_8);
    }

    public static byte @Nullable [] readBase64(JsonValue jsonValue, String key) {
        String b64 = readString(jsonValue, key);
        return b64 == null ? null : base64BasicDecode(b64);
    }

    public static @Nullable Integer getInteger(JsonValue v) {
        if (v.i != null) {
            return v.i;
        }
        // just in case the number was stored as a long, which is unlikely, but I want to handle it
        if (v.l != null && v.l <= Integer.MAX_VALUE && v.l >= Integer.MIN_VALUE) {
            return v.l.intValue();
        }
        return null;
    }

    public static @Nullable Long getLong(JsonValue v) {
        return v.l != null ? v.l
                : (v.i != null ? v.i.longValue() : null);
    }

    public static long getLong(JsonValue v, long dflt) {
        return v.l != null ? v.l
                : (v.i != null ? v.i.longValue() : dflt);
    }

    public static JsonValue instance(Duration d) {
        return new JsonValue(d.toNanos());
    }

    @SuppressWarnings("rawtypes")
    public static JsonValue instance(Collection list) {
        JsonValue v = new JsonValue(new ArrayList<>(list.size()));
        for (Object o : list) {
            v.array.add(toJsonValue(o));
        }
        return v;
    }

    public static JsonValue instance(Map<?,?> map) {
        JsonValue v = new JsonValue(new HashMap<>(map.size() *4/3+1));
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            v.map.put(entry.getKey().toString(), toJsonValue(entry.getValue()));
        }
        return v;
    }

    public static JsonValue toJsonValue(Object o) {
        if (o == null) {
            return JsonValue.NULL;
        }
        if (o instanceof JsonValue) {
            return (JsonValue)o;
        }
        if (o instanceof JsonSerializable) {
            return ((JsonSerializable)o).toJsonValue();
        }
        if (o instanceof Map) {
            //noinspection unchecked,rawtypes
            return new JsonValue((Map)o);
        }
        if (o instanceof List) {
            //noinspection unchecked,rawtypes
            return new JsonValue((List)o);
        }
        if (o instanceof Set) {
            //noinspection unchecked,rawtypes
            return new JsonValue(new ArrayList<>((Set)o));
        }
        if (o instanceof String) {
            String s = ((String)o).trim();
            return s.length() == 0 ? new JsonValue() : new JsonValue(s);
        }
        if (o instanceof Boolean) {
            return new JsonValue((Boolean)o);
        }
        if (o instanceof Integer) {
            return new JsonValue((Integer)o);
        }
        if (o instanceof Long) {
            return new JsonValue((Long)o);
        }
        if (o instanceof Double) {
            return new JsonValue((Double)o);
        }
        if (o instanceof Float) {
            return new JsonValue((Float)o);
        }
        if (o instanceof BigDecimal) {
            return new JsonValue((BigDecimal)o);
        }
        if (o instanceof BigInteger) {
            return new JsonValue((BigInteger)o);
        }
        return new JsonValue(o.toString());
    }

    public static MapBuilder mapBuilder() {
        return new MapBuilder();
    }

    public static class MapBuilder implements JsonSerializable {
        public JsonValue jv;

        public MapBuilder() {
            jv = new JsonValue(new HashMap<>());
        }

        public MapBuilder(JsonValue jv) {
            this.jv = jv;
        }

        public MapBuilder put(String s, Object o) {
            if (o != null) {
                JsonValue vv = JsonValueUtils.toJsonValue(o);
                if (vv.type != JsonValue.Type.NULL) {
                    jv.map.put(s, vv);
                    jv.mapOrder.add(s);
                }
            }
            return this;
        }

        public MapBuilder put(String key, Map<String, String> stringMap) {
            if (stringMap != null) {
                MapBuilder mb = new MapBuilder();
                for (Map.Entry<String, String> entry : stringMap.entrySet()) {
                    mb.put(entry.getKey(), entry.getValue());
                }
                jv.map.put(key, mb.jv);
                jv.mapOrder.add(key);
            }
            return this;
        }

        @Override
        public @NonNull String toJson() {
            return jv.toJson();
        }

        @Override
        public @NonNull JsonValue toJsonValue() {
            return jv;
        }

        @Deprecated public JsonValue getJsonValue() { return jv; }
    }
}
