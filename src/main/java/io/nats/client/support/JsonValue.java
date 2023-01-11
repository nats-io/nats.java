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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import static io.nats.client.support.JsonValueUtils.NULL_STR;
import static io.nats.client.support.JsonValueUtils.valueString;

public class JsonValue implements JsonSerializable {

    public enum Type {
        STRING, BOOL, OBJECT, ARRAY, INTEGER, LONG, DOUBLE, FLOAT, BIG_DECIMAL, BIG_INTEGER, NULL;
    }

    public static final JsonValue NULL = new JsonValue();
    public static final JsonValue TRUE = new JsonValue(true);
    public static final JsonValue FALSE = new JsonValue(false);
    public static final JsonValue EMPTY_OBJECT = new JsonValue(Collections.unmodifiableMap(new HashMap<>()));
    public static final JsonValue EMPTY_ARRAY = new JsonValue(Collections.unmodifiableList(new ArrayList<>()));

    public final Object object;
    public final Map<String, JsonValue> map;
    public final List<JsonValue> array;
    public final String string;
    public final Boolean bool;

    public final Number number;
    public final Integer i;
    public final Long l;
    public final Double d;
    public final Float f;
    public final BigDecimal bd;
    public final BigInteger bi;

    public final Type type;

    public JsonValue() {
        this(null, null, null, null, null, null, null, null, null, null);
    }

    public JsonValue(String string) {
        this(null, string, null, null, null, null, null, null, null, null);
    }

    public JsonValue(Boolean bool) {
        this(null, null, bool, null, null, null, null, null, null, null);
    }

    public JsonValue(Map<String, JsonValue> map) {
        this(map, null, null, null, null, null, null, null, null, null);
    }

    public JsonValue(int i) {
        this(null, null, null, i, null, null, null, null, null, null);
    }

    public JsonValue(long l) {
        this(null, null, null, null, l, null, null, null, null, null);
    }

    public JsonValue(double d) {
        this(null, null, null, null, null, d, null, null, null, null);
    }

    public JsonValue(float f) {
        this(null, null, null, null, null, null, f, null, null, null);
    }

    public JsonValue(BigDecimal bd) {
        this(null, null, null, null, null, null, null, bd, null, null);
    }

    public JsonValue(BigInteger bi) {
        this(null, null, null, null, null, null, null, null, bi, null);
    }

    public JsonValue(List<JsonValue> list) {
        this(null, null, null, null, null, null, null, null, null, list);
    }

    public JsonValue(JsonValue... values) {
        this(null, null, null, null, null, null, null, null, null,
            values == null ? null : Arrays.asList(values));
    }

    private JsonValue(Map<String, JsonValue> map, String string, Boolean bool, Integer i, Long l, Double d, Float f, BigDecimal bd, BigInteger bi, List<JsonValue> array) {
        this.map = map;
        this.array = array;
        this.string = string;
        this.bool = bool;
        this.i = i;
        this.l = l;
        this.d = d;
        this.f = f;
        this.bd = bd;
        this.bi = bi;
        if (i != null) {
            this.type = Type.INTEGER;
            number = i;
            object = number;
        }
        else if (l != null) {
            this.type = Type.LONG;
            number = l;
            object = number;
        }
        else if (d != null) {
            this.type = Type.DOUBLE;
            number = this.d;
            object = number;
        }
        else if (f != null) {
            this.type = Type.FLOAT;
            number = this.f;
            object = number;
        }
        else if (bd != null) {
            this.type = Type.BIG_DECIMAL;
            number = this.bd;
            object = number;
        }
        else if (bi != null) {
            this.type = Type.BIG_INTEGER;
            number = this.bi;
            object = number;
        }
        else {
            number = null;
            if (map != null) {
                this.type = Type.OBJECT;
                object = map;
            }
            else if (string != null) {
                this.type = Type.STRING;
                object = string;
            }
            else if (bool != null) {
                this.type = Type.BOOL;
                object = bool;
            }
            else if (array != null) {
                this.type = Type.ARRAY;
                object = array;
            }
            else {
                this.type = Type.NULL;
                object = null;
            }
        }
    }

    @Override
    public String toString() {
        return toJson();
    }

    @Override
    public String toJson() {
        switch (type) {
            case STRING:      return valueString(string);
            case BOOL:        return valueString(bool);
            case OBJECT:      return valueString(map);
            case ARRAY:       return valueString(array);
            case INTEGER:     return i.toString();
            case LONG:        return l.toString();
            case DOUBLE:      return d.toString();
            case FLOAT:       return f.toString();
            case BIG_DECIMAL: return bd.toString();
            case BIG_INTEGER: return bi.toString();
            default:          return NULL_STR;
        }
    }

    public String asString() {
        if (string == null) {
            return object == null ? NULL_STR : object.toString();
        }
        return string;
    }

    public Boolean asBool() {
        if (bool == null) {
            return Boolean.parseBoolean(asString());
        }
        return bool;
    }

    public int asInteger() {
        if (i == null) {
            if (number == null) {
                return Integer.parseInt(asString());
            }
            return number.intValue();
        }
        return i;
    }

    public long asLong() {
        if (l != null) { return l; }
        if (i != null) { return i; }
        if (number != null) { return number.longValue(); }
        return Long.parseLong(asString());
    }

    public double asDouble() {
        if (d == null) {
            if (number == null) {
                return Double.parseDouble(asString());
            }
            return number.doubleValue();
        }
        return d;
    }

    public float asFloat() {
        if (f == null) {
            if (number == null) {
                return Float.parseFloat(asString());
            }
            return number.floatValue();
        }
        return f;
    }

    public BigDecimal asBigDecimal() {
        if (bd == null) {
            if (number == null) {
                return new BigDecimal(asString());
            }
            return new BigDecimal(number.toString());
        }
        return bd;
    }

    public BigInteger asBigInteger() {
        if (bi == null) {
            if (number == null) {
                return new BigInteger(asString());
            }
            return new BigInteger(number.toString());
        }
        return bi;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JsonValue jsonValue = (JsonValue) o;

        if (type != jsonValue.type) return false;
        if (!Objects.equals(map, jsonValue.map)) return false;
        if (!Objects.equals(array, jsonValue.array)) return false;
        if (!Objects.equals(string, jsonValue.string)) return false;
        if (!Objects.equals(bool, jsonValue.bool)) return false;
        if (!Objects.equals(i, jsonValue.i)) return false;
        if (!Objects.equals(l, jsonValue.l)) return false;
        if (!Objects.equals(d, jsonValue.d)) return false;
        if (!Objects.equals(f, jsonValue.f)) return false;
        if (!Objects.equals(bd, jsonValue.bd)) return false;
        return Objects.equals(bi, jsonValue.bi);
    }

    @Override
    public int hashCode() {
        int result = map != null ? map.hashCode() : 0;
        result = 31 * result + (array != null ? array.hashCode() : 0);
        result = 31 * result + (string != null ? string.hashCode() : 0);
        result = 31 * result + (bool != null ? bool.hashCode() : 0);
        result = 31 * result + (i != null ? i.hashCode() : 0);
        result = 31 * result + (l != null ? l.hashCode() : 0);
        result = 31 * result + (d != null ? d.hashCode() : 0);
        result = 31 * result + (f != null ? f.hashCode() : 0);
        result = 31 * result + (bd != null ? bd.hashCode() : 0);
        result = 31 * result + (bi != null ? bi.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        return result;
    }
}
