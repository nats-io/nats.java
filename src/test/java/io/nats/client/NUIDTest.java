/**
 * 
 */
package io.nats.client;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.client.TestCasePrinterRule;

@Category(UnitTest.class)
public class NUIDTest {
	final Logger logger = LoggerFactory.getLogger(NUIDTest.class);

	@Rule
	public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGlobalNUID() {
		NUID n = NUID.getInstance();
		assertNotNull(n);
		assertNotNull("Expected prefix to be initialized", n.getPre());
		assertEquals(NUID.preLen, n.getPre().length);
		assertNotEquals("Expected seq to be non-zero", n.getSeq());
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
		NUID n = new NUID();
		
		long start = System.nanoTime();
		for (int i=0; i<count; i++) {
			String nuid = n.next();
		}
		long elapsedNsec = System.nanoTime() - start;
		System.err.printf("Average generation time for %d NUIDs was %fns\n", count, (double)elapsedNsec/count);
		
	}
	
	@Test
	public void testGlobalNUIDSpeed() {
		long count = 10000000;
		NUID n = NUID.getInstance();
		
		long start = System.nanoTime();
		for (int i=0; i<count; i++) {
			String nuid = n.next();
		}
		long elapsedNsec = System.nanoTime() - start;
		System.err.printf("Average generation time for %d global NUIDs was %fns\n", count, (double)elapsedNsec/count);
		
	}

//	@Test
//	public void testBasicUniqueness() {
//		int count = 10000000;
//		Map<String,Boolean> m = new HashMap<String,Boolean>(count);
//		
//		for (int i=0; i<count; i++) {
//			String n = NUID.nextGlobal();
//			if (m.get(n) != null) {
//				fail("Duplicate NUID found: " + n);
//			} else {
//				m.put(n, true);
//			}
//		}
//	}
}
