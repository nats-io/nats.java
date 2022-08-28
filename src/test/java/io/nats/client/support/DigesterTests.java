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

import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import static io.nats.client.support.Digester.DEFAULT_DIGEST_ALGORITHM;
import static io.nats.client.support.Digester.DEFAULT_STRING_ENCODING;
import static org.junit.jupiter.api.Assertions.assertEquals;

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

        assertEquals(dv1, dv2);
        assertEquals(dv1, dv3);
        assertEquals(dv1, dv4);

        digester1.reset();
        digester1.update(s);
        digester2.reset(s);
        digester3.reset(b);
        digester4.reset(b, 0, s.length());

        dv1 = digester1.getDigestValue();
        dv2 = digester2.getDigestValue();
        dv3 = digester3.getDigestValue();
        dv4 = digester4.getDigestValue();

        assertEquals(dv1, dv2);
        assertEquals(dv1, dv3);
        assertEquals(dv1, dv4);
    }
}