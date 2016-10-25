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

import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Locale;

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
        logger.info("Average generation time for {} NUIDs was {}ns",
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
        logger.info("Average generation time for {} global NUIDs was {}ns",
                NumberFormat.getNumberInstance(Locale.US).format(count),
                (double) elapsedNsec / count);
    }
}
