package io.nats.client.support;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;

import static io.nats.client.support.JsonUtils.*;

public class JsonValue implements JsonSerializable {
    public enum Type {
        OBJECT, ARRAY, STRING, BOOL, NULL, INTEGER, LONG, DOUBLE, FLOAT, BIG_DECIMAL, BIG_INTEGER;
    }

    public static final JsonValue NULL = new JsonValue(Type.NULL, null, null, null, null, null);
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

    public JsonValue(String object) {
        this(Type.STRING, object, null, null, object, null);
    }

    public JsonValue(Boolean object) {
        this(Type.BOOL, object, null, null, null, object);
    }

    public JsonValue(Map<String, JsonValue> map) {
        this(Type.OBJECT, map, map, null, null, null);
    }

    public JsonValue(List<JsonValue> array) {
        this(Type.ARRAY, array, null, array, null, null);
    }

    public JsonValue(int i) {
        this(Type.INTEGER, i, i, null, null, null, null, null);
    }

    public JsonValue(long l) {
        this(Type.LONG, l, null, l, null, null, null, null);
    }

    public JsonValue(double d) {
        this(Type.DOUBLE, d, null, null, d, null, null, null);
    }

    public JsonValue(float f) {
        this(Type.FLOAT, f, null, null, null, f, null, null);
    }

    public JsonValue(BigDecimal bd) {
        this(Type.BIG_DECIMAL, bd, null, null, null, null, bd, null);
    }

    public JsonValue(BigInteger bi) {
        this(Type.BIG_INTEGER, bi, null, null, null, null, null, bi);
    }

    private JsonValue(Type type, Object object, Map<String, JsonValue> map, List<JsonValue> array, String string, Boolean bool) {
        this.type = type;
        this.object = object;
        this.map = map;
        this.array = array;
        this.string = string;
        this.bool = bool;
        number = null;
        i = null;
        l = null;
        d = null;
        f = null;
        bd = null;
        bi = null;
    }

    private JsonValue(Type type, Number number, Integer i, Long l, Double d, Float f, BigDecimal bd, BigInteger bi) {
        this.type = type;
        object = number;
        map = null;
        array = null;
        string = null;
        bool = null;
        this.number = number;
        this.i = i;
        this.l = l;
        this.d = d;
        this.f = f;
        this.bd = bd;
        this.bi = bi;
    }

    @Override
    public String toString() {
        return toJson();
    }

    @SuppressWarnings("DataFlowIssue")
    @Override
    public String toJson() {
        switch (type) {
            case STRING:    return toJson(string);
            case BOOL:      return toJson(bool);
            case NULL:      return "null";
            case OBJECT:    return toJson(map);
            case ARRAY:     return toJson(array);
            default:        return object.toString();
        }
    }

    public static String toJson(String s) {
        return "\"" + Encoding.jsonEncode(s) + "\"";
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

    public static String toJson(List<JsonValue> array) {
        StringBuilder sba = beginArray();
        for (JsonValue v : array) {
            sba.append(v.toJson());
            sba.append(',');
        }
        return endArray(sba).toString();
    }

    public Boolean asBool() {
        if (bool == null) {
            throw new IllegalArgumentException("JsonValue cannot be represented as a boolean.");
        }
        return bool;
    }

    public int asInteger() {
        if (i == null) {
            if (number == null) {
                throw new IllegalArgumentException("JsonValue cannot be represented as an int.");
            }
            return number.intValue();
        }
        return i;
    }

    public long asLong() {
        if (l == null) {
            if (number == null) {
                throw new IllegalArgumentException("JsonValue cannot be represented as a long.");
            }
            return number.longValue();
        }
        return l;
    }

    public double asDouble() {
        if (d == null) {
            if (number == null) {
                throw new IllegalArgumentException("JsonValue cannot be represented as a double.");
            }
            return number.doubleValue();
        }
        return d;
    }

    public float asFloat() {
        if (f == null) {
            if (number == null) {
                throw new IllegalArgumentException("JsonValue cannot be represented as a float.");
            }
            return number.floatValue();
        }
        return f;
    }

    public BigDecimal asBigDecimal() {
        if (bd == null) {
            throw new IllegalArgumentException("JsonValue cannot be represented as a BigDecimal.");
        }
        return bd;
    }

    public BigInteger asBigInteger() {
        if (bi == null) {
            throw new IllegalArgumentException("JsonValue cannot be represented as a BigInteger.");
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
        return s == null ? null : DateTimeUtils.parseDateTime(s);
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
        return v != null && v.asBool();
    }

    public Boolean getMappedBoolean(String key, Boolean dflt) {
        JsonValue v = map == null ? null : map.get(key);
        return v == null ? dflt : v.asBool();
    }

    public Duration getMappedNanos(String key) {
        Long l = getMappedLong(key);
        return l == null ? null : Duration.ofNanos((l));
    }

    public Duration getMappedNanos(String key, Duration dflt) {
        Duration d = getMappedNanos(key);
        return d == null ? dflt : d;
    }

    public Duration getMappedDuration(String key) {
        JsonValue v = map == null ? null : map.get(key);
        return v == null ? null : Duration.ofNanos(v.asLong());
    }

    public Duration getMappedDuration(String key, Duration dflt) {
        JsonValue v = map == null ? null : map.get(key);
        return v == null ? dflt : Duration.ofNanos(v.asLong());
    }

    public List<Duration> getMappedDurationList(String key) {
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
        if (!Objects.equals(number, jsonValue.number)) return false;
        if (!Objects.equals(i, jsonValue.i)) return false;
        if (!Objects.equals(l, jsonValue.l)) return false;
        if (!Objects.equals(d, jsonValue.d)) return false;
        if (!Objects.equals(f, jsonValue.f)) return false;
        if (!Objects.equals(bd, jsonValue.bd)) return false;
        return Objects.equals(bi, jsonValue.bi);
    }

    @Override
    public int hashCode() {
        int result = object != null ? object.hashCode() : 0;
        result = 31 * result + (map != null ? map.hashCode() : 0);
        result = 31 * result + (array != null ? array.hashCode() : 0);
        result = 31 * result + (string != null ? string.hashCode() : 0);
        result = 31 * result + (bool != null ? bool.hashCode() : 0);
        result = 31 * result + (number != null ? number.hashCode() : 0);
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
