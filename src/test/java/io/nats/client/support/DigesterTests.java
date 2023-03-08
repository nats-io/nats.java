// Copyright 2022 The NATS Authors
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
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import static io.nats.client.support.Digester.DEFAULT_DIGEST_ALGORITHM;
import static io.nats.client.support.Digester.DEFAULT_STRING_ENCODING;
import static org.junit.jupiter.api.Assertions.*;

public final class DigesterTests {

    @Test
    public void testDigester() throws NoSuchAlgorithmException {
        Digester digester1 = new Digester();
        Digester digester2 = new Digester(Base64.getUrlEncoder());
        Digester digester3 = new Digester(DEFAULT_DIGEST_ALGORITHM);
        Digester digester4 = new Digester(DEFAULT_DIGEST_ALGORITHM, DEFAULT_STRING_ENCODING, Base64.getUrlEncoder());

        String s = "1234567890abcdefghijklmnopqrstuvwxyz~!@#$%^&*()_+1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ~!@#$%^&*()_+";
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        digester1.update(s);
        digester2.update(s);
        digester3.update(s);
        digester4.update(s);
        digester1.update(b);
        digester2.update(b);
        digester3.update(b);
        digester4.update(b);
        digester1.update(b, 0, s.length());
        digester2.update(b, 0, s.length());
        digester3.update(b, 0, s.length());
        digester4.update(b, 0, s.length());

        String dv1 = digester1.getDigestValue();
        String dv2 = digester2.getDigestValue();
        String dv3 = digester3.getDigestValue();
        String dv4 = digester4.getDigestValue();
        String de1 = digester1.getDigestEntry();
        String de2 = digester2.getDigestEntry();
        String de3 = digester3.getDigestEntry();
        String de4 = digester4.getDigestEntry();

        assertEquals(dv1, dv2);
        assertEquals(dv1, dv3);
        assertEquals(dv1, dv4);

        assertEquals(de1, de2);
        assertEquals(de1, de3);
        assertEquals(de1, de4);

        assertTrue(digester1.matches(de1));
        assertTrue(digester1.matches(de2));
        assertTrue(digester1.matches(de3));
        assertTrue(digester1.matches(de4));

        digester1.reset();
        digester1.update(s);
        digester2.reset(s);
        digester3.reset(b);
        digester4.reset(b, 0, s.length());

        dv1 = digester1.getDigestValue();
        dv2 = digester2.getDigestValue();
        dv3 = digester3.getDigestValue();
        dv4 = digester4.getDigestValue();

        de1 = digester1.getDigestEntry();
        de2 = digester2.getDigestEntry();
        de3 = digester3.getDigestEntry();
        de4 = digester4.getDigestEntry();

        assertEquals(dv1, dv2);
        assertEquals(dv1, dv3);
        assertEquals(dv1, dv4);

        assertEquals(de1, de2);
        assertEquals(de1, de3);
        assertEquals(de1, de4);

        assertTrue(digester1.matches(de1));
        assertTrue(digester1.matches(de2));
        assertTrue(digester1.matches(de3));
        assertTrue(digester1.matches(de4));

        assertFalse(digester1.matches("SHA-999="));
    }

    @Test
    public void testDigesterData() throws NoSuchAlgorithmException {
        String s = ResourceUtils.dataAsString("digester_test_bytes_000100.txt");
        Digester d = new Digester();
        d.update(s);
        assertEquals("IdgP4UYMGt47rgecOqFoLrd24AXukHf5-SVzqQ5Psg8=", d.getDigestValue());

        s = ResourceUtils.dataAsString("digester_test_bytes_001000.txt");
        d.reset(s);
        assertEquals("DZj4RnBpuEukzFIY0ueZ-xjnHY4Rt9XWn4Dh8nkNfnI=", d.getDigestValue());

        s = ResourceUtils.dataAsString("digester_test_bytes_010000.txt");
        d.reset(s);
        assertEquals("RgaJ-VSJtjNvgXcujCKIvaheiX_6GRCcfdRYnAcVy38=", d.getDigestValue());

        s = ResourceUtils.dataAsString("digester_test_bytes_100000.txt");
        d.reset(s);
        assertEquals("yan7pwBVnC1yORqqgBfd64_qAw6q9fNA60_KRiMMooE=", d.getDigestValue());

        d.reset("a");
        assertEquals("ypeBEsobvcr6wjGzmiPcTaeG7_gUfE5yuYB3ha_uSLs=", d.getDigestValue());

        d.reset("aa");
        assertEquals("lhtt0-3jy47LqsvWjeBAzXjrLtWIkTDM60xJJo6k1QY=", d.getDigestValue());

        d.reset("aaa");
        assertEquals("mDSHbc-wXLFnpcJJU-uljErImxrfV_KPL50JrxB-6PA=", d.getDigestValue());

        d.reset("aaaa");
        assertEquals("Yb5VqOL2tOFyM4vd8YTW2-4pyYhT4KBIXs7n8nua8LQ=", d.getDigestValue());
    }
}
