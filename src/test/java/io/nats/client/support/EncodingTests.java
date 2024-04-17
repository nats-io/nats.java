// Copyright 2015-2018 The NATS Authors
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

import io.nats.client.utils.ResourceUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static io.nats.client.support.Encoding.*;
import static io.nats.client.utils.ResourceUtils.dataAsLines;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public final class EncodingTests {
    @Test
    public void testJsonEncodeDecode() {
        _testJsonEncodeDecode("b4\\\\after", "b4\\after", null); // a single slash with a meaningless letter after it
        _testJsonEncodeDecode("b4\\\\tafter", "b4\\tafter", null); // a single slash with a char that can be part of an escape

        _testJsonEncodeDecode("b4\\bafter", "b4\bafter", null);
        _testJsonEncodeDecode("b4\\fafter", "b4\fafter", null);
        _testJsonEncodeDecode("b4\\nafter", "b4\nafter", null);
        _testJsonEncodeDecode("b4\\rafter", "b4\rafter", null);
        _testJsonEncodeDecode("b4\\tafter", "b4\tafter", null);

        _testJsonEncodeDecode("b4\\u0000after", "b4" + (char) 0 + "after", null);
        _testJsonEncodeDecode("b4\\u001fafter", "b4" + (char) 0x1f + "after", "b4\\u001fafter");
        _testJsonEncodeDecode("b4\\u0020after", "b4 after", "b4 after");
        _testJsonEncodeDecode("b4\\u0022after", "b4\"after", "b4\\\"after");
        _testJsonEncodeDecode("b4\\u0027after", "b4'after", "b4'after");
        _testJsonEncodeDecode("b4\\u003dafter", "b4=after", "b4=after");
        _testJsonEncodeDecode("b4\\u003Dafter", "b4=after", "b4=after");
        _testJsonEncodeDecode("b4\\u003cafter", "b4<after", "b4<after");
        _testJsonEncodeDecode("b4\\u003Cafter", "b4<after", "b4<after");
        _testJsonEncodeDecode("b4\\u003eafter", "b4>after", "b4>after");
        _testJsonEncodeDecode("b4\\u003Eafter", "b4>after", "b4>after");
        _testJsonEncodeDecode("b4\\u0060after", "b4`after", "b4`after");
        _testJsonEncodeDecode("b4\\xafter", "b4xafter", "b4xafter"); // unknown escape
        _testJsonEncodeDecode("b4\\", "b4\\", "b4\\\\"); // last char is \
        _testJsonEncodeDecode("b4\\/after", "b4/after", null);

        List<String> utfs = dataAsLines("utf8-only-no-ws-test-strings.txt");
        for (String u : utfs) {
            String uu = "b4\\b\\f\\n\\r\\t" + u + "after";
            _testJsonEncodeDecode(uu, "b4\b\f\n\r\t" + u + "after", uu);
        }
    }

    private void _testJsonEncodeDecode(String encodedInput, String targetDecode, String targetEncode) {
        String decoded = jsonDecode(encodedInput);
        assertEquals(targetDecode, decoded);
        String encoded = jsonEncode(new StringBuilder(), decoded).toString();
        if (targetEncode == null) {
            assertEquals(encodedInput, encoded);
        }
        else {
            assertEquals(targetEncode, encoded);
        }
    }

    @Test
    public void testBase64BasicEncoding() {
        String text = "blahblah";
        byte[] btxt = text.getBytes();

        byte[] encBytesFromBytes = base64BasicEncode(btxt);
        String encFromBytes = Encoding.base64BasicEncodeToString(btxt);
        String encFromString = Encoding.base64BasicEncodeToString(text);
        assertEquals("YmxhaGJsYWg=", new String(encBytesFromBytes));
        assertEquals("YmxhaGJsYWg=", encFromBytes);
        assertEquals("YmxhaGJsYWg=", encFromString);

        assertArrayEquals(btxt, base64BasicDecode(encBytesFromBytes));
        assertArrayEquals(btxt, base64BasicDecode(encFromBytes));
        assertEquals(text, base64BasicDecodeToString(encFromBytes));

        String data = ResourceUtils.dataAsString("digester_test_bytes_000100.txt");
        String check = ResourceUtils.dataAsString("digester_basic_encoded_000100.txt");
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        String enc = base64BasicEncodeToString(data.getBytes());
        assertEquals(check, enc);
        assertArrayEquals(bytes, base64BasicDecode(enc));

        data = ResourceUtils.dataAsString("digester_test_bytes_001000.txt");
        check = ResourceUtils.dataAsString("digester_basic_encoded_001000.txt");
        bytes = data.getBytes(StandardCharsets.UTF_8);
        enc = base64BasicEncodeToString(data.getBytes());
        assertEquals(check, enc);
        assertArrayEquals(bytes, base64BasicDecode(enc));

        data = ResourceUtils.dataAsString("digester_test_bytes_010000.txt");
        check = ResourceUtils.dataAsString("digester_basic_encoded_010000.txt");
        bytes = data.getBytes(StandardCharsets.UTF_8);
        enc = base64BasicEncodeToString(data.getBytes());
        assertEquals(check, enc);
        assertArrayEquals(bytes, base64BasicDecode(enc));

        data = ResourceUtils.dataAsString("digester_test_bytes_100000.txt");
        check = ResourceUtils.dataAsString("digester_basic_encoded_100000.txt");
        bytes = data.getBytes(StandardCharsets.UTF_8);
        enc = base64BasicEncodeToString(data.getBytes());
        assertEquals(check, enc);
        assertArrayEquals(bytes, base64BasicDecode(enc));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testBase64UrlEncoding() {
        String text = "blahblah";
        byte[] btxt = text.getBytes();
        String surl = "https://nats.io/";
        byte[] burl = surl.getBytes();

        byte[] encBytes = Encoding.base64Encode(btxt); //deprecated for base64UrlEncode
        byte[] uencBytes = Encoding.base64UrlEncode(btxt);
        assertEquals("YmxhaGJsYWg", base64UrlEncodeToString(text));
        assertEquals("YmxhaGJsYWg", new String(encBytes));
        assertEquals("YmxhaGJsYWg", new String(uencBytes));
        assertEquals("YmxhaGJsYWg", toBase64Url(text));   //deprecated
        assertEquals("YmxhaGJsYWg", toBase64Url(btxt));   //deprecated
        assertEquals(text, fromBase64Url("YmxhaGJsYWg")); //deprecated
        assertArrayEquals(btxt, base64UrlDecode(encBytes));
        assertArrayEquals(btxt, base64UrlDecode(uencBytes));

        //noinspection deprecation
        encBytes = Encoding.base64Encode(burl); //deprecated for base64UrlEncode
        uencBytes = Encoding.base64UrlEncode(burl);
        assertEquals("aHR0cHM6Ly9uYXRzLmlvLw", base64UrlEncodeToString(surl));
        assertEquals("aHR0cHM6Ly9uYXRzLmlvLw", new String(encBytes));
        assertEquals("aHR0cHM6Ly9uYXRzLmlvLw", new String(uencBytes));
        assertEquals("aHR0cHM6Ly9uYXRzLmlvLw", toBase64Url(surl)); //deprecated
        assertEquals("aHR0cHM6Ly9uYXRzLmlvLw", toBase64Url(burl)); //deprecated
        assertEquals(surl, fromBase64Url("aHR0cHM6Ly9uYXRzLmlvLw")); //deprecated
        assertArrayEquals(burl, base64UrlDecode(encBytes));
        assertArrayEquals(burl, base64UrlDecode(uencBytes));

        assertEquals("+ hello world", uriDecode("+%20hello%20world"));

        String data = ResourceUtils.dataAsString("digester_test_bytes_000100.txt");
        String check = ResourceUtils.dataAsString("digester_url_encoded_000100.txt");
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        String benc = base64UrlEncodeToString(bytes);
        assertEquals(check, benc);
        assertArrayEquals(bytes, base64UrlDecode(benc));

        data = ResourceUtils.dataAsString("digester_test_bytes_001000.txt");
        check = ResourceUtils.dataAsString("digester_url_encoded_001000.txt");
        bytes = data.getBytes(StandardCharsets.UTF_8);
        benc = base64UrlEncodeToString(bytes);
        assertEquals(check, benc);
        assertArrayEquals(bytes, base64UrlDecode(benc));

        data = ResourceUtils.dataAsString("digester_test_bytes_010000.txt");
        check = ResourceUtils.dataAsString("digester_url_encoded_010000.txt");
        bytes = data.getBytes(StandardCharsets.UTF_8);
        benc = base64UrlEncodeToString(bytes);
        assertEquals(check, benc);
        assertArrayEquals(bytes, base64UrlDecode(benc));

        data = ResourceUtils.dataAsString("digester_test_bytes_100000.txt");
        check = ResourceUtils.dataAsString("digester_url_encoded_100000.txt");
        bytes = data.getBytes(StandardCharsets.UTF_8);
        benc = base64UrlEncodeToString(bytes);
        assertEquals(check, benc);
        assertArrayEquals(bytes, base64UrlDecode(benc));
    }
}
