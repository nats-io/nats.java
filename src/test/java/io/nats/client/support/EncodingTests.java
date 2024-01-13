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

import org.junit.jupiter.api.Test;

import java.util.List;

import static io.nats.client.support.Encoding.*;
import static io.nats.client.utils.ResourceUtils.dataAsLines;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public final class EncodingTests {
    @Test
    public void testEncodeDecode() {
        _testEncodeDecode("b4\\\\after", "b4\\after", null); // a single slash with a meaningless letter after it
        _testEncodeDecode("b4\\\\tafter", "b4\\tafter", null); // a single slash with a char that can be part of an escape

        _testEncodeDecode("b4\\bafter", "b4\bafter", null);
        _testEncodeDecode("b4\\fafter", "b4\fafter", null);
        _testEncodeDecode("b4\\nafter", "b4\nafter", null);
        _testEncodeDecode("b4\\rafter", "b4\rafter", null);
        _testEncodeDecode("b4\\tafter", "b4\tafter", null);

        _testEncodeDecode("b4\\u0000after", "b4" + (char) 0 + "after", null);
        _testEncodeDecode("b4\\u001fafter", "b4" + (char) 0x1f + "after", "b4\\u001fafter");
        _testEncodeDecode("b4\\u0020after", "b4 after", "b4 after");
        _testEncodeDecode("b4\\u0022after", "b4\"after", "b4\\\"after");
        _testEncodeDecode("b4\\u0027after", "b4'after", "b4'after");
        _testEncodeDecode("b4\\u003dafter", "b4=after", "b4=after");
        _testEncodeDecode("b4\\u003Dafter", "b4=after", "b4=after");
        _testEncodeDecode("b4\\u003cafter", "b4<after", "b4<after");
        _testEncodeDecode("b4\\u003Cafter", "b4<after", "b4<after");
        _testEncodeDecode("b4\\u003eafter", "b4>after", "b4>after");
        _testEncodeDecode("b4\\u003Eafter", "b4>after", "b4>after");
        _testEncodeDecode("b4\\u0060after", "b4`after", "b4`after");
        _testEncodeDecode("b4\\xafter", "b4xafter", "b4xafter"); // unknown escape
        _testEncodeDecode("b4\\", "b4\\", "b4\\\\"); // last char is \
        _testEncodeDecode("b4\\/after", "b4/after", null);

        List<String> utfs = dataAsLines("utf8-only-no-ws-test-strings.txt");
        for (String u : utfs) {
            String uu = "b4\\b\\f\\n\\r\\t" + u + "after";
            _testEncodeDecode(uu, "b4\b\f\n\r\t" + u + "after", uu);
        }
    }

    private void _testEncodeDecode(String encodedInput, String targetDecode, String targetEncode) {
        String decoded = jsonDecode(encodedInput);
        assertEquals(targetDecode, decoded);
        String encoded = jsonEncode(new StringBuilder(), decoded).toString();
        if (targetEncode == null) {
            assertEquals(encodedInput, encoded);
        }
        else {
            assertEquals(targetEncode, encoded);
        }

        byte[] testBytes = decoded.getBytes();
        char[] e32 = base32Encode(testBytes);
        byte[] d32 = base32Decode(e32);
        assertArrayEquals(testBytes, d32);
    }
}
