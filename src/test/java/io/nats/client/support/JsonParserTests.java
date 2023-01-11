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

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
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
import static io.nats.client.support.JsonValueUtils.*;
import static io.nats.client.utils.ResourceUtils.dataAsLines;
import static io.nats.client.utils.TestBase.*;
import static org.junit.jupiter.api.Assertions.*;

public final class JsonParserTests {

    @Test
    public void testStringParsing() {
        List<String> encodeds = new ArrayList<>();
        List<String> decodeds = new ArrayList<>();
        Map<String, JsonValue> oMap = new HashMap<>();
        List<JsonValue> list = new ArrayList<>();

        int x = 0;
        addField(key(x++), "b4\\after", oMap, list, encodeds, decodeds);
        addField(key(x++), "b4/after", oMap, list, encodeds, decodeds);
        addField(key(x++), "b4\"after", oMap, list, encodeds, decodeds);
        addField(key(x++), "b4\tafter", oMap, list, encodeds, decodeds);
        addField(key(x++), "b4\\bafter", oMap, list, encodeds, decodeds);
        addField(key(x++), "b4\\fafter", oMap, list, encodeds, decodeds);
        addField(key(x++), "b4\\nafter", oMap, list, encodeds, decodeds);
        addField(key(x++), "b4\\rafter", oMap, list, encodeds, decodeds);
        addField(key(x++), "b4\\tafter", oMap, list, encodeds, decodeds);
        addField(key(x++), "b4" + (char) 0 + "after", oMap, list, encodeds, decodeds);
        addField(key(x++), "b4" + (char) 1 + "after", oMap, list, encodeds, decodeds);

        List<String> utfs = dataAsLines("utf8-only-no-ws-test-strings.txt");
        for (String u : utfs) {
            String uu = "b4\b\f\n\r\t" + u + "after";
            addField(key(x++), uu, oMap, list, encodeds, decodeds);
        }

        addField(key(x++), PLAIN, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_SPACE, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_PRINTABLE, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_DOT, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_STAR, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_GT, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_DASH, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_UNDER, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_DOLLAR, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_LOW, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_127, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_FWD_SLASH, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_BACK_SLASH, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_EQUALS, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_TIC, oMap, list, encodeds, decodeds);

        for (int i = 0; i < list.size(); i++) {
            JsonValue v = list.get(i);
            assertEquals(decodeds.get(i), v.string);
            assertEquals(v.toJson(), "\"" + encodeds.get(i) + "\"");
        }
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

    @Test
    public void testCoverage() {
        // coverage
        assertNull(JsonParser.parse(null));
        assertNull(JsonParser.parse(null, false));
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

        // some coverage here
        JsonValue vMap = new JsonValue(oMap);
        assertEquals(vMap.toJson(), vMap.toString());

        validateMapTypes(oMap, true, oMap);

        // don't keep nulls
        JsonValue parsed = JsonParser.parse(valueString(oMap));
        assertNotNull(parsed.map);
        assertEquals(oMap.size() - 1, parsed.map.size());
        validateMapTypes(parsed.map, false, oMap);

        // keep nulls
        parsed = JsonParser.parse(valueString(oMap), true);
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
    public void testArray() {
        List<JsonValue> list = new ArrayList<>();
        list.add(new JsonValue("string"));
        list.add(new JsonValue(true));
        list.add(JsonValue.NULL);
        list.add(JsonValue.EMPTY_OBJECT);
        list.add(JsonValue.EMPTY_ARRAY);

        JsonValue root = JsonParser.parse(valueString(list));
        assertNotNull(root.array);
        assertEquals(list.size(), root.array.size());
        List<JsonValue> array = root.array;
        for (int i = 0; i < array.size(); i++) {
            JsonValue v = array.get(i);
            JsonValue p = root.array.get(i);
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

        root = JsonParser.parse(valueString(list));
        assertNotNull(root.array);
        assertEquals(list.size(), root.array.size());
        array = root.array;
        for (int i = 0; i < array.size(); i++) {
            JsonValue v = array.get(i);
            JsonValue p = root.array.get(i);
            assertEquals(v.object, p.object);
            assertEquals(v.number, p.number);
        }

        Map<String, JsonValue> rootMap = new HashMap<>();
        rootMap.put("list", new JsonValue(list));
        rootMap.put("array", new JsonValue(list.toArray(new JsonValue[0])));
        root = new JsonValue(rootMap);
        List<JsonValue> mappedList = getMappedValue(root, "list").array;
        List<JsonValue> mappedList2 = JsonParser.parse(valueString(mappedList)).array;
        List<JsonValue> mappedArray = getMappedValue(root, "array").array;
        List<JsonValue> mappedArray2 = JsonParser.parse(valueString(list.toArray(new JsonValue[0]))).array;
        for (int i = 0; i < list.size(); i++) {
            JsonValue v = list.get(i);
            JsonValue lv = mappedList.get(i);
            JsonValue lv2 = mappedList2.get(i);
            JsonValue av = mappedArray.get(i);
            JsonValue av2 = mappedArray2.get(i);
            assertNotNull(lv);
            assertNotNull(lv2);
            assertNotNull(av);
            assertNotNull(av2);
            assertEquals(v, lv);
            assertEquals(v, av);

            // conversions are not perfect for doubles and floats, but that's a java thing, not a parser thing
            if (v.type == lv2.type) {
                assertEquals(v, lv2);
            }
            if (v.type == av2.type) {
                assertEquals(v, av2);
            }
        }
    }

    @Test
    public void testConstants() {
        assertThrows(UnsupportedOperationException.class, () -> JsonValue.EMPTY_OBJECT.map.put("foo", null));
        assertThrows(UnsupportedOperationException.class, () -> JsonValue.EMPTY_ARRAY.array.add(null));
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
        assertFalse(v.asBool());

        v = JsonValue.EMPTY_OBJECT;
        assertEquals("{}", v.asString());
        validateAsNotNumber(v);
        assertFalse(v.asBool());

        v = JsonValue.EMPTY_ARRAY;
        assertEquals("[]", v.asString());
        validateAsNotNumber(v);
        assertFalse(v.asBool());

        v = JsonValue.TRUE;
        assertEquals("true", v.asString());
        validateAsNotNumber(v);
        assertTrue(v.asBool());

        v = JsonValue.FALSE;
        assertEquals("false", v.asString());
        validateAsNotNumber(v);
        assertFalse(v.asBool());

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

        // coverage
        assertEquals(1, new JsonValue("1").asInteger());
        assertEquals(1, new JsonValue("1").asLong());
        assertEquals(1.2, new JsonValue("1.2").asDouble());
        assertEquals(1.2f, new JsonValue("1.2").asFloat());
        assertEquals(new BigDecimal("1.2"), new JsonValue("1.2").asBigDecimal());
        assertEquals(new BigInteger("1"), new JsonValue("1").asBigInteger());
    }

    private static void validateAsNotNumber(JsonValue v) {
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

        assertNotNull(getMappedValue(v, "string"));
        assertNull(getMappedValue(v, "x"));
        assertEquals(JsonValue.EMPTY_OBJECT, getMappedObjectOrEmpty(v, "x"));
        assertNull(getMapped(null, "na", vv -> vv));
        assertNull(getMapped(JsonValue.NULL, "na", vv -> vv));
        assertNull(getMapped(JsonValue.EMPTY_OBJECT, "na", vv -> vv));

        assertNull(getMappedDate(null, "na"));
        assertNull(getMappedDate(JsonValue.NULL, "na"));
        assertNull(getMappedDate(JsonValue.EMPTY_OBJECT, "na"));
        assertEquals(zdt, getMappedDate(v, "date"));
        assertNull(getMappedDate(v, "int"));

        assertFalse(getMappedBoolean(null, "na"));
        assertFalse(getMappedBoolean(null, "na", false));
        assertTrue(getMappedBoolean(null, "na", true));
        assertFalse(getMappedBoolean(JsonValue.NULL, "na"));
        assertFalse(getMappedBoolean(JsonValue.NULL, "na", false));
        assertTrue(getMappedBoolean(JsonValue.NULL, "na", true));
        assertFalse(getMappedBoolean(JsonValue.EMPTY_OBJECT, "na"));
        assertFalse(getMappedBoolean(JsonValue.EMPTY_OBJECT, "na", false));
        assertTrue(getMappedBoolean(JsonValue.EMPTY_OBJECT, "na", true));
        assertFalse(getMappedBoolean(v, "na"));
        assertFalse(getMappedBoolean(v, "na", false));
        assertTrue(getMappedBoolean(v, "na", true));
        assertFalse(getMappedBoolean(v, "int"));
        assertFalse(getMappedBoolean(v, "int", false));
        assertTrue(getMappedBoolean(v, "int", true));

        assertTrue(getMappedBoolean(v, "bool"));
        assertTrue(getMappedBoolean(v, "bool", false));
        assertFalse(getMappedBoolean(v, "x"));
        assertFalse(getMappedBoolean(v, "x", false));
        assertTrue(getMappedBoolean(v, "x", true));

        assertEquals("hello", getMappedString(v, "string"));
        assertEquals("hello", getMappedString(v, "string", null));
        assertNull(getMappedString(v, "x"));
        assertNull(getMappedString(v, "x", null));
        assertEquals("default", getMappedString(v, "x", "default"));
        assertNull(getMappedString(JsonValue.NULL, "x"));
        assertNull(getMappedString(JsonValue.NULL, "x", null));
        assertEquals("default", getMappedString(JsonValue.NULL, "x", "default"));

        assertEquals(zdt, getMappedDate(v, "date"));
        assertNull(getMappedDate(v, "x"));
        assertThrows(DateTimeException.class, () -> getMappedDate(v, "string"));

        assertEquals(dur, getMappedNanos(v, "dur"));
        assertEquals(dur, getMappedNanos(v, "dur", null));
        assertNull(getMappedNanos(v, "x"));
        assertNull(getMappedNanos(v, "x", null));
        assertEquals(dur2, getMappedNanos(v, "x", dur2));

        assertEquals(Integer.MAX_VALUE, getMappedInteger(v, "int"));
        assertEquals(Integer.MAX_VALUE, getMappedInteger(v, "int", Integer.MIN_VALUE));
        assertNull(getMappedInteger(v, "x"));
        assertEquals(Integer.MIN_VALUE, getMappedInteger(v, "x", Integer.MIN_VALUE));
        assertThrows(NumberFormatException.class, () -> getMappedInteger(v, "string"));

        assertEquals(Long.MAX_VALUE, getMappedLong(v, "long"));
        assertEquals(Long.MAX_VALUE, getMappedLong(v, "long", Long.MIN_VALUE));
        assertNull(getMappedLong(v, "x"));
        assertEquals(Long.MIN_VALUE, getMappedLong(v, "x", Long.MIN_VALUE));
        assertThrows(NumberFormatException.class, () -> getMappedLong(v, "string"));

        // these aren't maps
        JsonValue jvn = new JsonValue(1);
        JsonValue jvs = new JsonValue("s");
        JsonValue jvb = new JsonValue(true);
        JsonValue[] notMaps = new JsonValue[] {JsonValue.NULL, JsonValue.EMPTY_ARRAY, jvn, jvs, jvb};

        for (JsonValue vv : notMaps) {
            assertNull(getMappedValue(vv, "na"));
            assertEquals(JsonValue.EMPTY_OBJECT, getMappedObjectOrEmpty(vv, "na"));
            assertNull(getMappedDate(vv, "na"));
            assertNull(getMappedInteger(vv, "na"));
            assertEquals(-1, getMappedInteger(vv, "na", -1));
            assertNull(getMappedLong(vv, "na"));
            assertEquals(-2, getMappedLong(vv, "na", -2));
            assertFalse(getMappedBoolean(vv, "na"));
            assertNull(getMappedBoolean(vv, "na", null));
            assertTrue(getMappedBoolean(vv, "na", true));
            assertFalse(getMappedBoolean(vv, "na", false));
            assertNull(getMappedNanos(vv, "na"));
            assertEquals(Duration.ZERO, getMappedNanos(vv, "na", Duration.ZERO));
        }
    }

    @Test
    public void equalsContract() {
        Map<String, JsonValue> map1 = new HashMap<>();
        map1.put("1", new JsonValue(1));
        Map<String, JsonValue> map2 = new HashMap<>();
        map1.put("2", new JsonValue(2));
        List<JsonValue> list3 = new ArrayList<>();
        list3.add(new JsonValue(3));
        List<JsonValue> list4 = new ArrayList<>();
        list4.add(new JsonValue(4));
        EqualsVerifier.simple().forClass(JsonValue.class)
            .withPrefabValues(Map.class, map1, map2)
            .withPrefabValues(List.class, list3, list4)
            .withIgnoredFields("object", "number")
            .suppress(Warning.BIGDECIMAL_EQUALITY)
            .verify();
    }

    @Test
    public void testParsingCoverage() {
        JsonParseException e = assertThrows(JsonParseException.class, () -> JsonParser.parse("{"));
        assertTrue(e.getMessage().contains("Text must end with '}'"));

        e = assertThrows(JsonParseException.class, () -> JsonParser.parse("{{"));
        assertTrue(e.getMessage().contains("Cannot directly nest another Object or Array."));

        e = assertThrows(JsonParseException.class, () -> JsonParser.parse("{["));
        assertTrue(e.getMessage().contains("Cannot directly nest another Object or Array."));

        e = assertThrows(JsonParseException.class, () -> JsonParser.parse("{\"foo\":1 ]"));
        assertTrue(e.getMessage().contains("Expected a ',' or '}'."));

        e = assertThrows(JsonParseException.class, () -> JsonParser.parse("{\"foo\" 1"));
        assertTrue(e.getMessage().contains("Expected a ':' after a key."));

        e = assertThrows(JsonParseException.class, () -> JsonParser.parse("")); // no data
        assertTrue(e.getMessage().contains("Unexpected end of data."));

        e = assertThrows(JsonParseException.class, () -> JsonParser.parse("[\"bad\",")); // missing close
        assertTrue(e.getMessage().contains("Unexpected end of data."));

        e = assertThrows(JsonParseException.class, () -> JsonParser.parse("[1Z]")); // bad value
        assertTrue(e.getMessage().contains("Invalid value."));

        e = assertThrows(JsonParseException.class, () -> JsonParser.parse("t")); // bad value
        assertTrue(e.getMessage().contains("Invalid value."));

        e = assertThrows(JsonParseException.class, () -> JsonParser.parse("f")); // bad value
        assertTrue(e.getMessage().contains("Invalid value."));

        e = assertThrows(JsonParseException.class, () -> JsonParser.parse("\"u")); // bad value
        assertTrue(e.getMessage().contains("Unterminated string."));

        e = assertThrows(JsonParseException.class, () -> JsonParser.parse("\"u\r")); // bad value
        assertTrue(e.getMessage().contains("Unterminated string."));

        e = assertThrows(JsonParseException.class, () -> JsonParser.parse("\"u\n")); // bad value
        assertTrue(e.getMessage().contains("Unterminated string."));

        e = assertThrows(JsonParseException.class, () -> JsonParser.parse("\"\\x\"")); // bad value
        assertTrue(e.getMessage().contains("Illegal escape."));

        e = assertThrows(JsonParseException.class, () -> JsonParser.parse("\"\\u000")); // bad value
        assertTrue(e.getMessage().contains("Illegal escape."));

        e = assertThrows(JsonParseException.class, () -> JsonParser.parse("\"\\uzzzz")); // bad value
        assertTrue(e.getMessage().contains("Illegal escape."));

        JsonValue v = JsonParser.parse("{\"foo\":1,}");
        assertEquals(1, v.map.size());
        assertTrue(v.map.containsKey("foo"));

        v = JsonParser.parse("[\"foo\",]"); // handles dangling commas fine
        assertEquals(1, v.array.size());
        assertEquals("foo", v.array.get(0).string);

        String s = "foo \b \t \n \f \r \" \\ /";
        String j = "\"" + Encoding.jsonEncode(s) + "\"";
        v = JsonParser.parse(j);
        assertNotNull(v.string);
        assertEquals(s, v.string);
    }

    private void debug(JsonValue v) {
        System.out.println(v.type + " " + v);
    }

    @Test
    public void testNumberParsing() {
        assertEquals(JsonValue.Type.INTEGER, JsonParser.parse("1").type);
        assertEquals(JsonValue.Type.INTEGER, JsonParser.parse(Integer.toString(Integer.MAX_VALUE)).type);
        assertEquals(JsonValue.Type.INTEGER, JsonParser.parse(Integer.toString(Integer.MIN_VALUE)).type);
        assertEquals(JsonValue.Type.LONG, JsonParser.parse(Long.toString((long)Integer.MAX_VALUE + 1)).type);
        assertEquals(JsonValue.Type.LONG, JsonParser.parse(Long.toString((long)Integer.MIN_VALUE - 1)).type);
        assertEquals(JsonValue.Type.DOUBLE, JsonParser.parse("-0").type);
        assertEquals(JsonValue.Type.DOUBLE, JsonParser.parse("-0.0").type);
        assertEquals(JsonValue.Type.DOUBLE, JsonParser.parse("0.1d").type);
        assertEquals(JsonValue.Type.DOUBLE, JsonParser.parse("0.f").type);
        assertEquals(JsonValue.Type.DOUBLE, JsonParser.parse("0.1f").type);
        assertEquals(JsonValue.Type.DOUBLE, JsonParser.parse("-0x1.fffp1").type);
        assertEquals(JsonValue.Type.DOUBLE, JsonParser.parse("0x1.0P-1074").type);
        assertEquals(JsonValue.Type.BIG_DECIMAL, JsonParser.parse("0.2").type);
        assertEquals(JsonValue.Type.BIG_DECIMAL, JsonParser.parse("244273.456789012345").type);
        assertEquals(JsonValue.Type.BIG_DECIMAL, JsonParser.parse("244273.456789012345").type);
        assertEquals(JsonValue.Type.BIG_DECIMAL, JsonParser.parse("0.1234567890123456789").type);
        assertEquals(JsonValue.Type.BIG_DECIMAL, JsonParser.parse("-24.42e7345").type);
        assertEquals(JsonValue.Type.BIG_DECIMAL, JsonParser.parse("-24.42E7345").type);
        assertEquals(JsonValue.Type.BIG_DECIMAL, JsonParser.parse("-.01").type);
        assertEquals(JsonValue.Type.BIG_DECIMAL, JsonParser.parse("00.001").type);
        assertEquals(JsonValue.Type.BIG_INTEGER, JsonParser.parse("12345678901234567890").type);

        String str = new BigInteger( Long.toString(Long.MAX_VALUE) ).add( BigInteger.ONE ).toString();
        assertEquals(JsonValue.Type.BIG_INTEGER, JsonParser.parse(str).type);

        JsonParseException e = assertThrows(JsonParseException.class, () -> JsonParser.parse("-0x123"));
        assertTrue(e.getMessage().contains("Invalid value."));

        e = assertThrows(JsonParseException.class, () -> JsonParser.parse("-"));
        assertTrue(e.getMessage().contains("Invalid value."));

        e = assertThrows(JsonParseException.class, () -> JsonParser.parse("00"));
        assertTrue(e.getMessage().contains("Invalid value."));

        e = assertThrows(JsonParseException.class, () -> JsonParser.parse("NaN"));
        assertTrue(e.getMessage().contains("Invalid value."));

        e = assertThrows(JsonParseException.class, () -> JsonParser.parse("-NaN"));
        assertTrue(e.getMessage().contains("Invalid value."));

        e = assertThrows(JsonParseException.class, () -> JsonParser.parse("Infinity"));
        assertTrue(e.getMessage().contains("Invalid value."));

        e = assertThrows(JsonParseException.class, () -> JsonParser.parse("-Infinity"));
        assertTrue(e.getMessage().contains("Invalid value."));
    }
}
