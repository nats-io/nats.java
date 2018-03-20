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

package io.nats.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Locale;

public class NUIDTest extends BaseUnitTest {
    @Test
    @Category(UnitTest.class)
    public void testDigits() {
        if (NUID.digits.length != NUID.base) {
            fail("digits length does not match base modulo");
        }
    }

    @Test
    @Category(UnitTest.class)
    public void testGlobalNUIDInit() {
        NUID nuid = NUID.getInstance();
        assertNotNull(nuid);
        assertNotNull("Expected prefix to be initialized", nuid.getPre());
        assertEquals(NUID.preLen, nuid.getPre().length);
        assertNotEquals("Expected seq to be non-zero", 0, nuid.getSeq());
    }

    @Test
    @Category(UnitTest.class)
    public void testNUIDRollover() {
        NUID gnuid = NUID.getInstance();
        gnuid.setSeq(NUID.maxSeq);
        // copy
        char[] oldPre = Arrays.copyOf(gnuid.getPre(), gnuid.getPre().length);
        gnuid.next();
        assertNotEquals("Expected new pre, got the old one", oldPre, gnuid.getPre());
    }

    @Test
    @Category(UnitTest.class)
    public void testGUIDLen() {
        String nuid = new NUID().next();
        assertEquals(String.format("Expected len of %d, got %d", NUID.totalLen, nuid.length()),
                NUID.totalLen, nuid.length());
    }

    @Test(timeout = 5000)
    @Category(PerfTest.class)
    public void testProperPrefix() {
        char min = (char) 255;
        char max = (char) 0;
        char[] digits = NUID.digits;
        for (char digit : digits) {
            if (digit < min) {
                min = digit;
            }
            if (digit > max) {
                max = digit;
            }
        }

        int total = 100000;
        for (int i = 0; i < total; i++) {
            NUID nuid = new NUID();
            for (int j = 0; j < NUID.preLen; j++) {
                if (nuid.pre[j] < min || nuid.pre[j] > max) {
                    String msg = String.format(
                            "Iter %d. Valid range for bytes prefix: [%d..%d]\n"
                                    + "Incorrect prefix at pos %d: %s",
                            i, (int) min, (int) max, j, new String(nuid.pre));
                    fail(msg);
                }
            }
        }
    }

    @Test
    @Category(PerfTest.class)
    public void benchmarkNUIDSpeed() {
        long count = 10000000;
        NUID nuid = new NUID();

        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            nuid.next();
        }
        long elapsedNsec = System.nanoTime() - start;
        System.out.printf("Average generation time for %s NUIDs was %f ns\n",
                NumberFormat.getNumberInstance(Locale.US).format(count),
                (double) elapsedNsec / count);

    }

    @Test
    @Category(PerfTest.class)
    public void benchmarkGlobalNUIDSpeed() {
        long count = 10000000;
        NUID nuid = NUID.getInstance();

        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            nuid.next();
        }
        long elapsedNsec = System.nanoTime() - start;
        System.out.printf("Average generation time for %s global NUIDs was %f ns\n",
                NumberFormat.getNumberInstance(Locale.US).format(count),
                (double) elapsedNsec / count);
    }
}
