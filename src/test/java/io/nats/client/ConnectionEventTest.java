package io.nats.client;

import static io.nats.client.UnitTestUtilities.setLogLevel;
import static org.junit.Assert.assertEquals;

import ch.qos.logback.classic.Level;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(UnitTest.class)
public class ConnectionEventTest {
    static final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    static final Logger logger = LoggerFactory.getLogger(ConnectionEventTest.class);

    static final LogVerifier verifier = new LogVerifier();

    @Mock
    private Connection connMock;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);


    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        verifier.setup();
    }

    @After
    public void tearDown() throws Exception {
        verifier.teardown();
        setLogLevel(Level.INFO);
    }

    @Test
    public final void testConnectionEvent() {
        new ConnectionEvent(connMock);
    }

    @Test(expected = IllegalArgumentException.class)
    public final void testConnectionEventNull() {
        new ConnectionEvent(null);
    }

    @Test
    public final void testGetConnection() {
        ConnectionEvent cev = new ConnectionEvent(connMock);
        assertEquals(connMock, cev.getConnection());
    }
}
