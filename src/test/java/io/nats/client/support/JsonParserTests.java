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
        Map<String, JsonValue> rootMap = new HashMap<>();
        rootMap.put("map", new JsonValue(oMap));
        rootMap.put("list", new JsonValue(oList));
        JsonValue root = new JsonValue(rootMap);

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

        JsonValue clone = JsonParser.parse(root.toJson());
        List<JsonValue> mappedList = clone.getMappedArray("list");
        Map<String, JsonValue> mappedMap = clone.getMappedMap("map");
        int y = -1;
        for (JsonValue v : oList) {
            String key = key(++y);
            assertTrue(mappedList.contains(v));
            JsonValue mv = mappedMap.get(key);
            assertNotNull(mv);
            assertEquals(v, mv);
            assertEquals(decodeds.get(y), v.string);
            String vJson = v.toJson();
            assertEquals(vJson, "\"" + encodeds.get(y) + "\"");
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

        validateMapTypes(oMap, true, oMap);

        // don't keep nulls
        JsonValue parsed = JsonParser.parse(JsonValue.toJson(oMap));
        assertNotNull(parsed.map);
        assertEquals(oMap.size() - 1, parsed.map.size());

        // keep nulls
        parsed = JsonParser.parse(JsonValue.toJson(oMap), true);
        assertNotNull(parsed.map);
        assertEquals(oMap.size(), parsed.map.size());
        System.out.println("\n2 " + JsonValue.toJson(parsed.map));
        validateMapTypes(parsed.map, false, oMap);
    }

    private static void validateMapTypes(Map<String, JsonValue> map, boolean flag, Map<String, JsonValue> oMap) {
        assertEquals(JsonValue.Type.BOOL, map.get("trueKey1").type);
        assertEquals(JsonValue.Type.BOOL, map.get("trueKey2").type);
        assertEquals(JsonValue.Type.BOOL, map.get("falseKey1").type);
        assertEquals(JsonValue.Type.BOOL, map.get("falseKey2").type);
        assertEquals(JsonValue.Type.STRING, map.get("stringKey").type);
        assertEquals(JsonValue.Type.STRING, map.get("escapeStringKey").type);
        assertEquals(JsonValue.Type.NULL, map.get("nullKey").type);
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
        assertNull(map.get("nullKey").object);
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
        assertEquals(oMap.get("nullKey"), map.get("nullKey"));

        assertEquals(oMap.get("intKey1"), map.get("intKey1"));
        assertEquals(oMap.get("intKey2"), map.get("intKey2"));
        assertEquals(oMap.get("longKey1"), map.get("longKey1"));
        assertEquals(oMap.get("longKey2"), map.get("longKey2"));
        assertEquals(oMap.get("zzzzzzzzzz"), map.get("zzzzzzzz"));

        if (flag) {
            assertEquals(JsonValue.Type.DOUBLE, map.get("doubleKey1").type);
            assertEquals(JsonValue.Type.DOUBLE, map.get("doubleKey2").type);
            assertEquals(JsonValue.Type.FLOAT, map.get("floatKey1").type);
            assertEquals(JsonValue.Type.FLOAT, map.get("floatKey2").type);

            assertNotNull(map.get("doubleKey1").d);
            assertNotNull(map.get("doubleKey2").d);
            assertNotNull(map.get("floatKey1").f);
            assertNotNull(map.get("floatKey2").f);

            assertEquals(oMap.get("doubleKey1"), map.get("doubleKey1"));
            assertEquals(oMap.get("doubleKey2"), map.get("doubleKey2"));
            assertEquals(oMap.get("floatKey1"), map.get("floatKey1"));
            assertEquals(oMap.get("floatKey2"), map.get("floatKey2"));
        }
    }

    @Test
    public void testHeterogeneousArray() {
        List<JsonValue> list = new ArrayList<>();
        list.add(new JsonValue("string"));
        list.add(new JsonValue(true));
        list.add(JsonValue.NULL);
        list.add(JsonValue.EMPTY_OBJECT);
        list.add(JsonValue.EMPTY_ARRAY);
        list.add(new JsonValue(1));
        list.add(new JsonValue(Long.MAX_VALUE));
        list.add(new JsonValue(Double.MAX_VALUE));
        list.add(new JsonValue(Float.MAX_VALUE));
        list.add(new JsonValue(new BigDecimal(Double.toString(Double.MAX_VALUE))));
        list.add(new JsonValue(new BigInteger(Integer.toString(Integer.MAX_VALUE))));
    }
}
