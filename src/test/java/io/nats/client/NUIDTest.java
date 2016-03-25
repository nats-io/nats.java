/**
 * 
 */
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Category(UnitTest.class)
public class NUIDTest {
    final Logger logger = LoggerFactory.getLogger(NUIDTest.class);

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {}

    @Test
    public void testGlobalNUID() {
        NUID nuid = NUID.getInstance();
        assertNotNull(nuid);
        assertNotNull("Expected prefix to be initialized", nuid.getPre());
        assertEquals(NUID.preLen, nuid.getPre().length);
        assertNotEquals("Expected seq to be non-zero", nuid.getSeq());
    }

    @Test
    public void testNUIDRollover() {
        NUID gnuid = NUID.getInstance();
        gnuid.setSeq(NUID.maxSeq);
        // copy
        char[] oldPre = Arrays.copyOf(gnuid.getPre(), gnuid.getPre().length);
        gnuid.next();
        assertNotEquals("Expected new pre, got the old one", oldPre, gnuid.getPre());
    }

    @Test
    public void testNUIDLen() {
        String nuid = new NUID().next();
        assertEquals(String.format("Expected len of %d, got %d", NUID.totalLen, nuid.length()),
                NUID.totalLen, nuid.length());
    }

    @Test
    public void testNUIDSpeed() {
        long count = 10000000;
        NUID nuid = new NUID();

        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            nuid.next();
        }
        long elapsedNsec = System.nanoTime() - start;
        logger.info("Average generation time for {} NUIDs was {}ns", count,
                (double) elapsedNsec / count);

    }

    @Test
    public void testGlobalNUIDSpeed() {
        long count = 10000000;
        NUID nuid = NUID.getInstance();

        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            nuid.next();
        }
        long elapsedNsec = System.nanoTime() - start;
        logger.info("Average generation time for {} global NUIDs was {}ns", count,
                (double) elapsedNsec / count);

    }

    @Test
    public void testBasicUniqueness() {
        int count = 10000000;
        Map<String, Boolean> nuidMap = new HashMap<String, Boolean>(count);

        for (int i = 0; i < count; i++) {
            // String n = NUID.nextGlobal();
            String nuid = new NUID().next();
            if (nuidMap.get(nuid) != null) {
                fail("Duplicate NUID found: " + nuid);
            } else {
                nuidMap.put(nuid, true);
            }
        }
    }

    @Test
    public void testProperPrefix() {
        char min = (char) 255;
        char max = (char) 0;
        char[] digits = NUID.digits;
        for (int i = 0; i < digits.length; i++) {
            if (digits[i] < min) {
                min = digits[i];
            }
            if (digits[i] > max) {
                max = digits[i];
            }
        }

        int total = 100000;
        for (int i = 0; i < total; i++) {
            NUID nuid = new NUID();
            for (int j = 0; j < NUID.preLen; j++) {
                if (nuid.pre[j] < min || nuid.pre[j] > max) {
                    String msg = String.format(
                            "Iter %d. Valid range for bytes prefix: [%d..%d]\n"
                                    + "Incorrect prefix at pos %d: %v (%s)",
                            i, min, max, j, nuid.pre, new String(nuid.pre));
                    fail(msg);
                }
            }
        }

    }
}
