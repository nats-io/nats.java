package io.nats.client;

import ch.qos.logback.classic.Logger;
import io.nats.examples.Requestor;
import io.nats.examples.Replier;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.nats.client.UnitTestUtilities.runDefaultServer;
import static io.nats.client.UnitTestUtilities.sleep;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class ReplierTest {
    static final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    static final Logger logger = (Logger) LoggerFactory.getLogger(ReplierTest.class);

    static final LogVerifier verifier = new LogVerifier();

    ExecutorService service = Executors.newCachedThreadPool();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

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
    public void testReplierStringArray() throws Exception {
        List<String> argList = new ArrayList<String>();
        argList.addAll(Arrays.asList("-s", Nats.DEFAULT_URL));
        argList.add("foo");
        argList.add("got it");

        String[] args = new String[argList.size()];
        args = argList.toArray(args);

        new Replier(args);
    }

    @Test
    public void testParseArgsBadFlags() {
        List<String> argList = new ArrayList<String>();
        String[] flags = new String[] { "-s", "--server", "-n", "--count", "-q", "--qgroup" };
        boolean exThrown = false;

        for (String flag : flags) {
            try {
                exThrown = false;
                argList.clear();
                argList.addAll(Arrays.asList(flag, "foo", "gotcha"));
                String[] args = new String[argList.size()];
                args = argList.toArray(args);
                new Replier(args);
            } catch (IllegalArgumentException e) {
                assertEquals(String.format("%s requires an argument", flag), e.getMessage());
                exThrown = true;
            } finally {
                assertTrue("Should have thrown exception", exThrown);
            }
        }
    }

    @Test
    public void testParseArgsNotEnoughArgs() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("must supply at least a subject name and a reply msg");

        List<String> argList = new ArrayList<String>();

        argList.clear();
        String[] args = new String[argList.size()];
        args = argList.toArray(args);
        new Replier(args);
    }

    @Test(timeout = 5000)
    public void testMainSuccess() throws Exception {
        final List<Throwable> errors = new ArrayList<Throwable>();
        try (NatsServer srv = runDefaultServer()) {
            final CountDownLatch startReq = new CountDownLatch(1);
            final CountDownLatch done = new CountDownLatch(1);
            service.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Replier.main(new String[]{"-s", Nats.DEFAULT_URL, "-n", "1", "foo", "gotcha"});
                        done.countDown();
                    } catch (Exception e) {
                        errors.add(e);
                    }
                }
            });
            sleep(500);
            startReq.countDown();
            service.execute(new Runnable() {
                public void run() {
                    try {
                        startReq.await();
                        new Requestor(new String[]{"-s", Nats.DEFAULT_URL, "foo", "bar"}).run();
                    } catch (Exception e) {
                        errors.add(e);
                    }
                }
            });

            done.await();
            if (errors.size() != 0) {
                for (Throwable t : errors) {
                    t.printStackTrace();
                }
                fail("Unexpected exceptions");
            }
        }
    }

    @Test
    public void testMainFailsNoServers() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(Nats.ERR_NO_SERVERS);
        Replier.main(new String[] { "-s", "nats://enterprise:4242", "foobar", "gotcha" });
    }
}
