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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.nats.client.support.JsonUtils.addField;
import static io.nats.client.support.JsonUtils.beginArray;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endArray;
import static io.nats.client.support.JsonUtils.endJson;

public class JsonValue implements JsonSerializable {

    public enum Type {
        STRING, BOOL,  INTEGER, LONG, DOUBLE, FLOAT, BIG_DECIMAL, BIG_INTEGER,  MAP, ARRAY, NULL;
    }

    private static final char QUOTE = '"';
    private static final char COMMA = ',';
    private static final String NULL_STR = "null";

    public static final JsonValue NULL = new JsonValue();
    public static final JsonValue TRUE = new JsonValue(true);
    public static final JsonValue FALSE = new JsonValue(false);
    public static final JsonValue EMPTY_MAP = new JsonValue(Collections.emptyMap());
    public static final JsonValue EMPTY_ARRAY = new JsonValue(Collections.emptyList());

    public final Type type;
    public final Object value;
    public final List<String> mapOrder;

    public JsonValue() {
        this((Object) null);
    }

    public JsonValue(String string) {
        this((Object) string);
    }

    public JsonValue(Boolean bool) {
        this((Object) bool);
    }

    public JsonValue(int i) {
        this((Object) i);
    }

    public JsonValue(long l) {
        this((Object) l);
    }

    public JsonValue(double d) {
        this((Object) d);
    }

    public JsonValue(float f) {
        this((Object) f);
    }

    public JsonValue(BigDecimal bd) {
        this((Object) bd);
    }

    public JsonValue(BigInteger bi) {
        this((Object) bi);
    }

    public JsonValue(Map<String, JsonValue> map) {
        this((Object) map);
    }

    public JsonValue(@Nullable List<JsonValue> list) {
        this((Object) list);
    }

    public JsonValue(JsonValue[] values) {
        this((Object)(values == null ? null : Arrays.asList(values)));
    }

    private JsonValue(@Nullable Object value) {
        this.value = value;
        mapOrder = new ArrayList<>();
        if (value instanceof Integer) {
            this.type = Type.INTEGER;
        }
        else if (value instanceof Long) {
            this.type = Type.LONG;
        }
        else if (value instanceof Double) {
            this.type = Type.DOUBLE;
        }
        else if (value instanceof Float) {
            this.type = Type.FLOAT;
        }
        else if (value instanceof BigDecimal) {
            this.type = Type.BIG_DECIMAL;
        }
        else if (value instanceof BigInteger) {
            this.type = Type.BIG_INTEGER;
        }
        else if (value instanceof Map) {
            this.type = Type.MAP;
        }
        else if (value instanceof String) {
            this.type = Type.STRING;
        }
        else if (value instanceof Boolean) {
            this.type = Type.BOOL;
        }
        else if (value instanceof List) {
            this.type = Type.ARRAY;
        }
        else if (value == null){
            this.type = Type.NULL;
        }
        else {
            throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName() + " for value: " + value);
        }
    }

    public String toString(Class<?> c) {
        return toString(c.getSimpleName());
    }

    public String toString(String key) {
        return QUOTE + key + QUOTE +':'+ toJson();
    }

    @Override
    public String toString() {
        return toJson();
    }

    @Override
    @NonNull
    public JsonValue toJsonValue() {
        return this;
    }

    public @Nullable String string () {
        switch (type) {
            case STRING: case NULL: return (String) value;
            //todo case INTEGER: case LONG: case DOUBLE: case BIG_DECIMAL: case BIG_INTEGER: return value.toString();
            default: return null;
        }
    }

    public @Nullable Boolean bool () {
        switch (type) {
            case BOOL: case NULL: return (Boolean) value;
            //todo case INTEGER: case LONG: case DOUBLE: case BIG_DECIMAL: case BIG_INTEGER: return ((Number) value).longValue() != 0;
            default: return null;
        }
    }

    @SuppressWarnings("unchecked")
    public @Nullable Map<String, JsonValue> map () {
        switch (type) {
            case MAP: case NULL: return (Map<String, JsonValue>) value;
            default: return null;
        }
    }

    @SuppressWarnings("unchecked")
    public @Nullable List<JsonValue> array () {
        switch (type) {
            case ARRAY: case NULL: return (List<JsonValue>) value;
            default: return null;
        }
    }

    public @Nullable Integer i () {
        return getInteger();
    }

    public @Nullable Long l () {
        return getLong();
    }

    public @Nullable Float f () {
        if (value instanceof Float) {
            return (Float)value;
        }
        return value instanceof Number ? ((Number) value).floatValue() : null;
    }

    public @Nullable Double d () {
        if (value instanceof Double) {
            return (Double) value;
        }
        return value instanceof Number ? ((Number) value).doubleValue() : null;
    }

    public @Nullable BigDecimal bd () {
        if (value instanceof BigDecimal) {
            return (BigDecimal)value;
        }
        return value instanceof Number ? new BigDecimal(value.toString()) : null;
    }

    public @Nullable BigInteger bi () {
        if (value instanceof BigInteger) {
            return (BigInteger)value;
        }
        if (value instanceof BigDecimal) {
            return ((BigDecimal)value).toBigInteger();
        }
        return value instanceof Number ? new BigInteger(Long.toString(((Number) value).longValue())) : null;
    }

    public @Nullable Integer getInteger() {
        if (value instanceof Integer) {
            return (Integer)value;
        }
        // just in case the number was stored as a long, which is unlikely, but I want to handle it
        if (value instanceof Number) {
            long x = ((Number) value).longValue();
            if (x <= Integer.MAX_VALUE && x >= Integer.MIN_VALUE) {
                return (int) x;
            }
        }
        return null;
    }

    public @Nullable Long getLong() {
        if (value instanceof Long) {
            return (Long)value;
        }
        return value instanceof Number ? ((Number) value).longValue() : null;
    }

    public long getLong(long dflt) {
        if (value instanceof Long) {
            return (Long)value;
        }
        return value instanceof Number ? ((Number) value).longValue() : dflt;
    }

    @SuppressWarnings("unchecked")
    public int size () {
        switch (type) {
            case MAP:   return ((Map<String, JsonValue>) value).size();
            case ARRAY: return ((Collection<JsonValue>) value).size();
            case NULL:  return 0;
            default:    return 1;
        }
    }

    @Override
    @NonNull
    public String toJson() {
        switch (type) {
            case STRING:      return valueString(string());
            case BOOL:        return valueString(bool());
            case MAP:         return valueString(map());
            case ARRAY:       return valueString(array());
            case NULL:        return NULL_STR;
            default:          return value.toString();
        }
    }

    private String valueString(String s) {
        return QUOTE + Encoding.jsonEncode(s) + QUOTE;
    }

    private String valueString(boolean b) {
        return Boolean.toString(b).toLowerCase();
    }

    private String valueString(Map<String, JsonValue> map) {
        StringBuilder sbo = beginJson();
        if (!mapOrder.isEmpty()) {
            for (String key : mapOrder) {
                addField(sbo, key, map.get(key));
            }
        }
        else {
            for (Map.Entry<String, JsonValue> entry : map.entrySet()) {
                addField(sbo, entry.getKey(), entry.getValue());
            }
        }
        return endJson(sbo).toString();
    }

    private String valueString(List<JsonValue> list) {
        StringBuilder sba = beginArray();
        for (JsonValue v : list) {
            sba.append(v.toJson());
            sba.append(COMMA);
        }
        return endArray(sba).toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof JsonValue)) return false;

        JsonValue jsonValue = (JsonValue) o;

        if (type != jsonValue.type) return false;
        return Objects.equals(value, jsonValue.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(value);
        result = 31 * result + Objects.hashCode(type);
        return result;
    }
}
