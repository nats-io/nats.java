import org.junit.Test;
import static org.junit.Assert.*;

public class ConnectTest {
    @Test(expected = UnsupportedOperationException.class)
    public void testDefaultConnection() {
        Connection nc = Nats.connect();
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testConnection() {
        Connection nc = Nats.connect("foo");
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testConnectionWithOptions() {
        Options options = new Options();
        Connection nc = Nats.connect("foo", options);
    }
}