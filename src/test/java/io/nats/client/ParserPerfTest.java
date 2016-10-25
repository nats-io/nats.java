package io.nats.client;

import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ParserPerfTest {
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
    public void test() {
        try (TcpConnectionMock mock = new TcpConnectionMock()) {
            ConnectionFactory cf = new ConnectionFactory();
            try (ConnectionImpl conn = cf.createConnection(new TcpConnectionFactoryMock())) {
                final int BUF_SIZE = 65536;
                int count = 40000;

                Parser p = new Parser(conn);

                byte[] buf = new byte[BUF_SIZE];

                String msg = "MSG foo 1 4\r\ntest\r\n";
                byte[] msgBytes = msg.getBytes();
                int length = msgBytes.length;

                int bufLen = 0;
                int numMsgs = 0;
                for (int i = 0; (i + length) <= BUF_SIZE; i += length, numMsgs++) {
                    System.arraycopy(msgBytes, 0, buf, i, length);
                    bufLen += length;
                }

                System.err.printf("Parsing %d buffers of %d messages each (total=%d)\n", count,
                        numMsgs, count * numMsgs);

                long t0 = System.nanoTime();
                for (int i = 0; i < count; i++) {
                    try {
                        p.parse(buf, bufLen);
                    } catch (ParseException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        System.err.println("Error offset=" + e.getErrorOffset());
                        break;
                    }
                }
                long elapsed = System.nanoTime() - t0;
                long avgNsec = elapsed / (count * numMsgs);
                long elapsedSec = TimeUnit.NANOSECONDS.toSeconds(elapsed);
                // long elapsedMsec = TimeUnit.NANOSECONDS.toMicros(elapsedNanos);

                long totalMsgs = numMsgs * count;
                System.err.printf("Parsed %d messages in %ds (%d msg/sec)\n", totalMsgs, elapsedSec,
                        (totalMsgs / elapsedSec));

                double totalBytes = (double) count * bufLen;
                double mbPerSec = (double) totalBytes / elapsedSec / 1000000;
                System.err.printf("Parsed %.0fMB in %ds (%.0fMB/sec)\n", totalBytes / 1000000,
                        elapsedSec, mbPerSec);

                System.err.printf("Average parse time per msg = %dns\n", avgNsec);
            } catch (IOException | TimeoutException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                fail(e.getMessage());
            }
        }
    }

    /**
     * Main executive.
     * 
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        ParserPerfTest parserPerfTest = new ParserPerfTest();

        // b.testPubSpeed();
        parserPerfTest.test();
    }

}
