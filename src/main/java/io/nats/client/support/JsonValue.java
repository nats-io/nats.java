package io.nats.client.support;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;

import static io.nats.client.support.JsonUtils.*;

public class JsonValue implements JsonSerializable {

    private static final char COMMA = ',';
    private static final String NULL_STR = "null";
    private static final String QUOTE = "\"";
    private static final String EMPTY_STRING = "";

    public enum Type {
        STRING, BOOL, OBJECT, ARRAY, INTEGER, LONG, DOUBLE, FLOAT, BIG_DECIMAL, BIG_INTEGER, NULL;
    }

    public static final JsonValue NULL = new JsonValue();
    public static final JsonValue EMPTY_OBJECT = new JsonValue(new HashMap<>());
    public static final JsonValue EMPTY_ARRAY = new JsonValue(new ArrayList<>());

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
            case STRING:      return toJson(string);
            case BOOL:        return toJson(bool);
            case NULL:        return NULL_STR;
            case OBJECT:      return toJson(map);
            case ARRAY:       return toJson(array);
            case INTEGER:     return i.toString();
            case LONG:        return l.toString();
            case DOUBLE:      return d.toString();
            case FLOAT:       return f.toString();
            case BIG_DECIMAL: return bd.toString();
            case BIG_INTEGER: return bi.toString();
        }
        return EMPTY_STRING;
    }

    public static String toJson(String s) {
        return QUOTE + Encoding.jsonEncode(s) + QUOTE;
    }

    public static String toJson(boolean b) {
        return Boolean.toString(b).toLowerCase();
    }

    public static String toJson(Map<String, JsonValue> map) {
        StringBuilder sbo = beginJson();
        for (String key : map.keySet()) {
            addField(sbo, key, map.get(key));
        }
        return endJson(sbo).toString();
    }

    public static String toJson(List<JsonValue> list) {
        StringBuilder sba = beginArray();
        for (JsonValue v : list) {
            sba.append(v.toJson());
            sba.append(COMMA);
        }
        return endArray(sba).toString();
    }

    public static String toJson(JsonValue[] array) {
        StringBuilder sba = beginArray();
        for (JsonValue v : array) {
            sba.append(v.toJson());
            sba.append(COMMA);
        }
        return endArray(sba).toString();
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
        if (l == null) {
            if (number == null) {
                return Long.parseLong(asString());
            }
            return number.longValue();
        }
        return l;
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

    public JsonValue getMappedObject(String key) {
        return map == null ? null : map.get(key);
    }

    public JsonValue getMappedObjectOrEmpty(String key) {
        if (map == null) {
            return EMPTY_OBJECT;
        }
        JsonValue v = map.get(key);
        return v == null ? EMPTY_OBJECT : v;
    }

    public Map<String, JsonValue> getMappedMap(String key) {
        if (map == null) {
            return null;
        }
        JsonValue v = map.get(key);
        return v == null || v.map == null || v.map.size() == 0 ? null : v.map;
    }

    public JsonValue[] getMappedArrayAsArray(String key) {
        if (map == null) {
            return null;
        }
        JsonValue v = map.get(key);
        if (v == null || v.array == null || v.array.size() == 0) {
            return null;
        }
        return v.array.toArray(new JsonValue[0]);
    }

    public List<JsonValue> getMappedArray(String key) {
        if (map == null) {
            return null;
        }
        JsonValue v = map.get(key);
        return v == null || v.array == null || v.array.size() == 0 ? null : v.array;
    }

    public String getMappedString(String key) {
        JsonValue v = map == null ? null : map.get(key);
        return v == null ? null : v.string;
    }

    public String getMappedString(String key, String dflt) {
        JsonValue v = map == null ? null : map.get(key);
        return v == null ? dflt : v.string;
    }

    public List<String> getMappedStringList(String key) {
        List<String> list = new ArrayList<>();
        JsonValue v = map == null ? null : map.get(key);
        if (v != null && v.array != null) {
            for (JsonValue jv : v.array) {
                list.add(jv.string);
            }
        }
        return list;
    }

    public ZonedDateTime getMappedDate(String key) {
        String s = getMappedString(key);
        return s == null ? null : DateTimeUtils.parseDateTimeThrowParseError(s);
    }

    public Integer getMappedInteger(String key) {
        JsonValue v = map == null ? null : map.get(key);
        return v == null ? null : v.asInteger();
    }

    public int getMappedInteger(String key, int dflt) {
        JsonValue v = map == null ? null : map.get(key);
        return v == null ? dflt : v.asInteger();
    }

    public Long getMappedLong(String key) {
        JsonValue v = map == null ? null : map.get(key);
        return v == null ? null : v.asLong();
    }

    public long getMappedLong(String key, long dflt) {
        JsonValue v = map == null ? null : map.get(key);
        return v == null ? dflt : v.asLong();
    }

    public boolean getMappedBoolean(String key) {
        JsonValue v = map == null ? null : map.get(key);
        return v != null && v.type == Type.BOOL ? v.bool : false;
    }

    public Boolean getMappedBoolean(String key, Boolean dflt) {
        JsonValue v = map == null ? null : map.get(key);
        return v != null && v.type == Type.BOOL ? v.bool : dflt;
    }

    public Duration getMappedNanos(String key) {
        Long l = getMappedLong(key);
        return l == null ? null : Duration.ofNanos((l));
    }

    public Duration getMappedNanos(String key, Duration dflt) {
        Duration d = getMappedNanos(key);
        return d == null ? dflt : d;
    }

    public List<Duration> getMappedNanosList(String key) {
        List<Duration> list = new ArrayList<>();
        JsonValue v = map == null ? null : map.get(key);
        if (v != null && v.array != null) {
            for (JsonValue jv : v.array) {
                list.add(Duration.ofNanos(jv.asLong()));
            }
        }
        return list;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JsonValue jsonValue = (JsonValue) o;

        if (type != jsonValue.type) return false;
        if (!Objects.equals(object, jsonValue.object)) return false;
        if (!Objects.equals(map, jsonValue.map)) return false;
        if (!Objects.equals(array, jsonValue.array)) return false;
        if (!Objects.equals(string, jsonValue.string)) return false;
        if (!Objects.equals(bool, jsonValue.bool)) return false;
        return Objects.equals(number, jsonValue.number);
//        if (!Objects.equals(number, jsonValue.number)) return false;
//        if (!Objects.equals(i, jsonValue.i)) return false;
//        if (!Objects.equals(l, jsonValue.l)) return false;
//        if (!Objects.equals(d, jsonValue.d)) return false;
//        if (!Objects.equals(f, jsonValue.f)) return false;
//        if (!Objects.equals(bd, jsonValue.bd)) return false;
//        return Objects.equals(bi, jsonValue.bi);
    }

    @Override
    public int hashCode() {
        int result = object != null ? object.hashCode() : 0;
        result = 31 * result + (map != null ? map.hashCode() : 0);
        result = 31 * result + (array != null ? array.hashCode() : 0);
        result = 31 * result + (string != null ? string.hashCode() : 0);
        result = 31 * result + (bool != null ? bool.hashCode() : 0);
        result = 31 * result + (number != null ? number.hashCode() : 0);
//        result = 31 * result + (i != null ? i.hashCode() : 0);
//        result = 31 * result + (l != null ? l.hashCode() : 0);
//        result = 31 * result + (d != null ? d.hashCode() : 0);
//        result = 31 * result + (f != null ? f.hashCode() : 0);
//        result = 31 * result + (bd != null ? bd.hashCode() : 0);
//        result = 31 * result + (bi != null ? bi.hashCode() : 0);
//        result = 31 * result + (type != null ? type.hashCode() : 0);
        return result;
    }
}
