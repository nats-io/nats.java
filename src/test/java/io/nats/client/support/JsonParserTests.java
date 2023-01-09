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

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.nats.client.support.Encoding.jsonEncode;
import static io.nats.client.utils.ResourceUtils.dataAsLines;
import static io.nats.client.utils.TestBase.*;
import static org.junit.jupiter.api.Assertions.*;

public final class JsonParserTests {

    @Test
    public void testStringParsing() {
        int x = 0;
        List<String> encodeds = new ArrayList<>();
        List<String> decodeds = new ArrayList<>();
        Map<String, JsonValue> oMap = new HashMap<>();
        List<JsonValue> oList = new ArrayList<>();

        addField(key(x++), "b4\\after", oMap, oList, encodeds, decodeds);
        addField(key(x++), "b4/after", oMap, oList, encodeds, decodeds);
        addField(key(x++), "b4\"after", oMap, oList, encodeds, decodeds);
        addField(key(x++), "b4\tafter", oMap, oList, encodeds, decodeds);
        addField(key(x++), "b4\\bafter", oMap, oList, encodeds, decodeds);
        addField(key(x++), "b4\\fafter", oMap, oList, encodeds, decodeds);
        addField(key(x++), "b4\\nafter", oMap, oList, encodeds, decodeds);
        addField(key(x++), "b4\\rafter", oMap, oList, encodeds, decodeds);
        addField(key(x++), "b4\\tafter", oMap, oList, encodeds, decodeds);
        addField(key(x++), "b4" + (char)0 + "after", oMap, oList, encodeds, decodeds);
        addField(key(x++), "b4" + (char)1 + "after", oMap, oList, encodeds, decodeds);

        List<String> utfs = dataAsLines("utf8-only-no-ws-test-strings.txt");
        for (String u : utfs) {
            String uu = "b4\b\f\n\r\t" + u + "after";
            addField(key(x++), uu, oMap, oList, encodeds, decodeds);
        }

        addField(key(x++), PLAIN, oMap, oList, encodeds, decodeds);
        addField(key(x++), HAS_SPACE, oMap, oList, encodeds, decodeds);
        addField(key(x++), HAS_PRINTABLE, oMap, oList, encodeds, decodeds);
        addField(key(x++), HAS_DOT, oMap, oList, encodeds, decodeds);
        addField(key(x++), HAS_STAR, oMap, oList, encodeds, decodeds);
        addField(key(x++), HAS_GT, oMap, oList, encodeds, decodeds);
        addField(key(x++), HAS_DASH, oMap, oList, encodeds, decodeds);
        addField(key(x++), HAS_UNDER, oMap, oList, encodeds, decodeds);
        addField(key(x++), HAS_DOLLAR, oMap, oList, encodeds, decodeds);
        addField(key(x++), HAS_LOW, oMap, oList, encodeds, decodeds);
        addField(key(x++), HAS_127, oMap, oList, encodeds, decodeds);
        addField(key(x++), HAS_FWD_SLASH, oMap, oList, encodeds, decodeds);
        addField(key(x++), HAS_BACK_SLASH, oMap, oList, encodeds, decodeds);
        addField(key(x++), HAS_EQUALS, oMap, oList, encodeds, decodeds);
        addField(key(x++), HAS_TIC, oMap, oList, encodeds, decodeds);

        Map<String, JsonValue> rootMap = new HashMap<>();
        rootMap.put("map", new JsonValue(oMap));
        rootMap.put("list", new JsonValue(oList));
        rootMap.put("array", new JsonValue(oList.toArray(new JsonValue[0])));
        JsonValue root = new JsonValue(rootMap);

        JsonValue clone = JsonParser.parse(root.toJson());
        List<JsonValue> mappedList = clone.getMappedArray("list");
        JsonValue[] mappedArray = clone.getMappedArrayAsArray("array");
        Map<String, JsonValue> mappedMap = clone.getMappedMap("map");
        for (int i = 0; i < oList.size(); i++) {
            JsonValue v = oList.get(i);
            String key = key(i);
            JsonValue lv = mappedList.get(i);
            JsonValue av = mappedArray[i];
            JsonValue mv = mappedMap.get(key);
            assertNotNull(lv);
            assertNotNull(av);
            assertNotNull(mv);
            assertEquals(v, lv);
            assertEquals(v, av);
            assertEquals(v, mv);
            assertEquals(decodeds.get(i), v.string);
            assertEquals(v.toJson(), "\"" + encodeds.get(i) + "\"");
        }
    }

    private String key(int x) {
        return "x\tz" + x;
    }

    private void addField(String name, String decoded,
                          Map<String, JsonValue> map, List<JsonValue> list,
                          List<String> encodeds, List<String> decodeds) {
        String enc = jsonEncode(decoded);
        encodeds.add(enc);
        decodeds.add(decoded);
        JsonValue jv = new JsonValue(decoded);
        map.put(name, jv);
        list.add(jv);
    }

    @SuppressWarnings("UnnecessaryUnicodeEscape")
    @Test
    public void testJsonValuePrimitives() {
        Map<String, JsonValue> oMap = new HashMap<>();
        oMap.put("trueKey1", new JsonValue(true));
        oMap.put("trueKey2", new JsonValue(Boolean.TRUE));
        oMap.put("falseKey1", new JsonValue(false));
        oMap.put("falseKey2", new JsonValue(Boolean.FALSE));
        oMap.put("stringKey", new JsonValue("hello world!"));
        oMap.put("escapeStringKey", new JsonValue("h\be\tllo w\u1234orld!"));
        oMap.put("nullKey", JsonValue.NULL);
        oMap.put("intKey1", new JsonValue(Integer.MAX_VALUE));
        oMap.put("intKey2", new JsonValue(Integer.MIN_VALUE));
        oMap.put("longKey1", new JsonValue(Long.MAX_VALUE));
        oMap.put("longKey2", new JsonValue(Long.MIN_VALUE));
        oMap.put("doubleKey1", new JsonValue(Double.MAX_VALUE));
        oMap.put("doubleKey2", new JsonValue(Double.MIN_VALUE));
        oMap.put("floatKey1", new JsonValue(Float.MAX_VALUE));
        oMap.put("floatKey2", new JsonValue(Float.MIN_VALUE));
        oMap.put("bigDecimalKey1", new JsonValue(new BigDecimal("9223372036854775807.123")));
        oMap.put("bigDecimalKey2", new JsonValue(new BigDecimal("-9223372036854775808.123")));
        oMap.put("bigIntegerKey1", new JsonValue(new BigInteger("9223372036854775807")));
        oMap.put("bigIntegerKey2", new JsonValue(new BigInteger("-9223372036854775808")));

        validateMapTypes(oMap, true, oMap);

        // don't keep nulls
        JsonValue parsed = JsonParser.parse(JsonValue.toJson(oMap));
        assertNotNull(parsed.map);
        assertEquals(oMap.size() - 1, parsed.map.size());
        validateMapTypes(parsed.map, false, oMap);

        // keep nulls
        parsed = JsonParser.parse(JsonValue.toJson(oMap), true);
        assertNotNull(parsed.map);
        assertEquals(oMap.size(), parsed.map.size());
        validateMapTypes(parsed.map, true, oMap);
    }

    private static void validateMapTypes(Map<String, JsonValue> map, boolean checkNull, Map<String, JsonValue> oMap) {
        assertEquals(JsonValue.Type.BOOL, map.get("trueKey1").type);
        assertEquals(JsonValue.Type.BOOL, map.get("trueKey2").type);
        assertEquals(JsonValue.Type.BOOL, map.get("falseKey1").type);
        assertEquals(JsonValue.Type.BOOL, map.get("falseKey2").type);
        assertEquals(JsonValue.Type.STRING, map.get("stringKey").type);
        assertEquals(JsonValue.Type.STRING, map.get("escapeStringKey").type);
        assertEquals(JsonValue.Type.INTEGER, map.get("intKey1").type);
        assertEquals(JsonValue.Type.INTEGER, map.get("intKey2").type);
        assertEquals(JsonValue.Type.LONG, map.get("longKey1").type);
        assertEquals(JsonValue.Type.LONG, map.get("longKey2").type);

        assertNotNull(map.get("trueKey1").bool);
        assertNotNull(map.get("trueKey2").bool);
        assertNotNull(map.get("falseKey1").bool);
        assertNotNull(map.get("falseKey2").bool);
        assertNotNull(map.get("stringKey").string);
        assertNotNull(map.get("escapeStringKey").string);
        assertNotNull(map.get("intKey1").i);
        assertNotNull(map.get("intKey2").i);
        assertNotNull(map.get("longKey1").l);
        assertNotNull(map.get("longKey2").l);

        assertEquals(oMap.get("trueKey1"), map.get("trueKey1"));
        assertEquals(oMap.get("trueKey2"), map.get("trueKey2"));
        assertEquals(oMap.get("falseKey1"), map.get("falseKey1"));
        assertEquals(oMap.get("falseKey2"), map.get("falseKey2"));
        assertEquals(oMap.get("stringKey"), map.get("stringKey"));
        assertEquals(oMap.get("escapeStringKey"), map.get("escapeStringKey"));
        assertEquals(oMap.get("intKey1"), map.get("intKey1"));
        assertEquals(oMap.get("intKey2"), map.get("intKey2"));
        assertEquals(oMap.get("longKey1"), map.get("longKey1"));
        assertEquals(oMap.get("longKey2"), map.get("longKey2"));

        if (checkNull) {
            assertEquals(JsonValue.Type.NULL, map.get("nullKey").type);
            assertNull(map.get("nullKey").object);
            assertEquals(oMap.get("nullKey"), map.get("nullKey"));
        }

        assertNotNull(oMap.get("intKey1").number);
        assertNotNull(oMap.get("intKey2").number);
        assertNotNull(oMap.get("longKey1").number);
        assertNotNull(oMap.get("longKey2").number);
        assertNotNull(oMap.get("doubleKey1").number);
        assertNotNull(oMap.get("doubleKey2").number);
        assertNotNull(oMap.get("floatKey1").number);
        assertNotNull(oMap.get("floatKey2").number);
        assertNotNull(oMap.get("bigDecimalKey1").number);
        assertNotNull(oMap.get("bigDecimalKey2").number);
        assertNotNull(oMap.get("bigIntegerKey1").number);
        assertNotNull(oMap.get("bigIntegerKey2").number);

        assertEquals(oMap.get("intKey1").asInteger(), map.get("intKey1").asInteger());
        assertEquals(oMap.get("intKey2").asInteger(), map.get("intKey2").asInteger());
        assertEquals(oMap.get("longKey1").asLong(), map.get("longKey1").asLong());
        assertEquals(oMap.get("longKey2").asLong(), map.get("longKey2").asLong());
        assertEquals(oMap.get("doubleKey1").asDouble(), map.get("doubleKey1").asDouble());
        assertEquals(oMap.get("doubleKey2").asDouble(), map.get("doubleKey2").asDouble());
        assertEquals(oMap.get("floatKey1").asFloat(), map.get("floatKey1").asFloat());
        assertEquals(oMap.get("floatKey2").asFloat(), map.get("floatKey2").asFloat());
        assertEquals(oMap.get("bigDecimalKey1").asBigDecimal(), map.get("bigDecimalKey1").asBigDecimal());
        assertEquals(oMap.get("bigDecimalKey2").asBigDecimal(), map.get("bigDecimalKey2").asBigDecimal());
        assertEquals(oMap.get("bigIntegerKey1").asBigInteger(), map.get("bigIntegerKey1").asBigInteger());
        assertEquals(oMap.get("bigIntegerKey2").asBigInteger(), map.get("bigIntegerKey2").asBigInteger());
    }

    @Test
    public void testHeterogeneousArray() {
        List<JsonValue> list = new ArrayList<>();
        list.add(new JsonValue("string"));
        list.add(new JsonValue(true));
        list.add(JsonValue.NULL);
        list.add(JsonValue.EMPTY_OBJECT);
        list.add(JsonValue.EMPTY_ARRAY);

        JsonValue parsed = JsonParser.parse(JsonValue.toJson(list));
        assertNotNull(parsed.array);
        assertEquals(list.size(), parsed.array.size());
        List<JsonValue> array = parsed.array;
        for (int i = 0; i < array.size(); i++) {
            JsonValue v = array.get(i);
            JsonValue p = parsed.array.get(i);
            assertEquals(v.object, p.object);
            assertTrue(list.contains(v));
        }

        list.clear();
        list.add(new JsonValue(1));
        list.add(new JsonValue(Long.MAX_VALUE));
        list.add(new JsonValue(Double.MAX_VALUE));
        list.add(new JsonValue(Float.MAX_VALUE));
        list.add(new JsonValue(new BigDecimal(Double.toString(Double.MAX_VALUE))));
        list.add(new JsonValue(new BigInteger(Long.toString(Long.MAX_VALUE))));

        parsed = JsonParser.parse(JsonValue.toJson(list));
        assertNotNull(parsed.array);
        assertEquals(list.size(), parsed.array.size());
        array = parsed.array;
        for (int i = 0; i < array.size(); i++) {
            JsonValue v = array.get(i);
            JsonValue p = parsed.array.get(i);
            assertEquals(v.object, p.object);
            assertEquals(v.number, p.number);
        }
    }

    @Test
    public void testNull() {
        assertEquals(JsonValue.Type.NULL, JsonValue.NULL.type);
        assertNull(JsonValue.NULL.object);
        assertNull(JsonValue.NULL.map);
        assertNull(JsonValue.NULL.array);
        assertNull(JsonValue.NULL.string);
        assertNull(JsonValue.NULL.bool);
        assertNull(JsonValue.NULL.number);
        assertNull(JsonValue.NULL.i);
        assertNull(JsonValue.NULL.l);
        assertNull(JsonValue.NULL.d);
        assertNull(JsonValue.NULL.f);
        assertNull(JsonValue.NULL.bd);
        assertNull(JsonValue.NULL.bi);
        assertEquals(JsonValue.NULL, new JsonValue((String)null));
        assertEquals(JsonValue.NULL, new JsonValue((Boolean) null));
        assertEquals(JsonValue.NULL, new JsonValue((Map<String, JsonValue>)null));
        assertEquals(JsonValue.NULL, new JsonValue((List<JsonValue>)null));
        assertEquals(JsonValue.NULL, new JsonValue((JsonValue[])null));
        assertEquals(JsonValue.NULL, new JsonValue((BigDecimal)null));
        assertEquals(JsonValue.NULL, new JsonValue((BigInteger) null));
    }

    @Test
    public void testAs() {
        JsonValue v = JsonValue.NULL;
        assertEquals("null", v.asString());
        validateAsNotNumber(v);

        v = JsonValue.EMPTY_OBJECT;
        assertEquals("{}", v.asString());
        validateAsNotNumber(v);

        v = JsonValue.EMPTY_ARRAY;
        assertEquals("[]", v.asString());
        validateAsNotNumber(v);

        validateAsNumber(new JsonValue(Integer.MAX_VALUE));
        validateAsNumber(new JsonValue(Integer.MIN_VALUE));
        validateAsNumber(new JsonValue(Long.MAX_VALUE));
        validateAsNumber(new JsonValue(Long.MIN_VALUE));
        validateAsNumber(new JsonValue(Double.MAX_VALUE));
        validateAsNumber(new JsonValue(Double.MIN_VALUE));
        validateAsNumber(new JsonValue(Float.MAX_VALUE));
        validateAsNumber(new JsonValue(Float.MIN_VALUE));
        validateAsNumber(new JsonValue(new BigDecimal("9223372036854775807.123")));
        validateAsNumber(new JsonValue(new BigDecimal("-9223372036854775808.123")));
        validateAsNumber(new JsonValue(new BigInteger("9223372036854775807")));
        validateAsNumber(new JsonValue(new BigInteger("-9223372036854775808")));
    }

    private static void validateAsNotNumber(JsonValue v) {
        assertFalse(v.asBool());
        assertThrows(NumberFormatException.class, v::asInteger);
        assertThrows(NumberFormatException.class, v::asLong);
        assertThrows(NumberFormatException.class, v::asDouble);
        assertThrows(NumberFormatException.class, v::asFloat);
        assertThrows(NumberFormatException.class, v::asBigDecimal);
        assertThrows(NumberFormatException.class, v::asBigInteger);
    }

    private static void validateAsNumber(JsonValue v) {
        assertFalse(v.asBool());
        v.asInteger();
        v.asLong();
        v.asDouble();
        v.asFloat();
        v.asBigDecimal();
        if (v.type == JsonValue.Type.INTEGER || v.type == JsonValue.Type.LONG || v.type == JsonValue.Type.BIG_INTEGER) {
            v.asBigInteger();
        }
    }

    @Test
    public void testGetMapped() {
        ZonedDateTime zdt = DateTimeUtils.gmtNow();
        Duration dur = Duration.ofNanos(4273);
        Duration dur2 = Duration.ofNanos(7342);

        JsonValue v = new JsonValue(new HashMap<>());
        v.map.put("bool", new JsonValue(Boolean.TRUE));
        v.map.put("string", new JsonValue("hello"));
        v.map.put("int", new JsonValue(Integer.MAX_VALUE));
        v.map.put("long", new JsonValue(Long.MAX_VALUE));
        v.map.put("date", new JsonValue(DateTimeUtils.toRfc3339(zdt)));
        v.map.put("dur", new JsonValue(dur.toNanos()));
        v.map.put("strings", new JsonValue(new JsonValue("s1"), new JsonValue("s2")));
        v.map.put("durs", new JsonValue(new JsonValue(dur.toNanos()), new JsonValue(dur2.toNanos())));

        assertNotNull(v.getMappedObject("string"));
        assertNull(v.getMappedObject("x"));
        assertEquals(JsonValue.EMPTY_OBJECT, v.getMappedObjectOrEmpty("x"));

        assertTrue(v.getMappedBoolean("bool"));
        assertTrue(v.getMappedBoolean("bool", false));
        assertFalse(v.getMappedBoolean("x"));
        assertFalse(v.getMappedBoolean("x", false));
        assertTrue(v.getMappedBoolean("x", true));

        assertEquals("hello", v.getMappedString("string"));
        assertEquals("hello", v.getMappedString("string", null));
        assertNull(v.getMappedString("x"));
        assertNull(v.getMappedString("x", null));
        assertEquals("default", v.getMappedString("x", "default"));

        assertEquals(zdt, v.getMappedDate("date"));
        assertNull(v.getMappedDate("x"));
        assertThrows(DateTimeException.class, () -> v.getMappedDate("string"));

        assertEquals(dur, v.getMappedNanos("dur"));
        assertEquals(dur, v.getMappedNanos("dur", null));
        assertNull(v.getMappedNanos("x"));
        assertNull(v.getMappedNanos("x", null));
        assertEquals(dur2, v.getMappedNanos("x", dur2));

        assertEquals(Integer.MAX_VALUE, v.getMappedInteger("int"));
        assertEquals(Integer.MAX_VALUE, v.getMappedInteger("int", Integer.MIN_VALUE));
        assertNull(v.getMappedInteger("x"));
        assertEquals(Integer.MIN_VALUE, v.getMappedInteger("x", Integer.MIN_VALUE));
        assertThrows(NumberFormatException.class, () -> v.getMappedInteger("string"));

        assertEquals(Long.MAX_VALUE, v.getMappedLong("long"));
        assertEquals(Long.MAX_VALUE, v.getMappedLong("long", Long.MIN_VALUE));
        assertNull(v.getMappedLong("x"));
        assertEquals(Long.MIN_VALUE, v.getMappedLong("x", Long.MIN_VALUE));
        assertThrows(NumberFormatException.class, () -> v.getMappedLong("string"));

        // these aren't maps
        JsonValue jvn = new JsonValue(1);
        JsonValue jvs = new JsonValue("s");
        JsonValue jvb = new JsonValue(true);
        JsonValue[] notMaps = new JsonValue[] {JsonValue.NULL, JsonValue.EMPTY_ARRAY, jvn, jvs, jvb};

        for (JsonValue vv : notMaps) {
            assertNull(vv.getMappedObject("na"));
            assertEquals(JsonValue.EMPTY_OBJECT, vv.getMappedObjectOrEmpty("na"));
            assertNull(vv.getMappedDate("na"));
            assertNull(vv.getMappedInteger("na"));
            assertEquals(-1, vv.getMappedInteger("na", -1));
            assertNull(vv.getMappedLong("na"));
            assertEquals(-2, vv.getMappedLong("na", -2));
            assertFalse(vv.getMappedBoolean("na"));
            assertNull(vv.getMappedBoolean("na", null));
            assertTrue(vv.getMappedBoolean("na", true));
            assertFalse(vv.getMappedBoolean("na", false));
            assertNull(vv.getMappedNanos("na"));
            assertEquals(Duration.ZERO, vv.getMappedNanos("na", Duration.ZERO));
        }
    }
}
