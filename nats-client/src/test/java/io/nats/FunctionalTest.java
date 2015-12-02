package io.nats;

import io.nats.imp.ClientConnection;
import io.nats.imp.Context;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class FunctionalTest
{
  private Context ctx;
  private io.nats.imp.ClientConnection theConnection;

  @BeforeTest
  public void setupTest ()
    throws NatsException
  {
    ctx = new Context ();
    theConnection = (io.nats.imp.ClientConnection) ctx.connect ();
  }

  @Test
  public void testNewInbox ()
    throws Exception
  {
    assert (theConnection != null);
    Destination mb1 = theConnection.newInbox (0xdeadbeefL);
    Destination mb2 = theConnection.newInbox (0xdeadbeefL);
    Destination check = theConnection.destination (mb1.name ());

    assert ! mb1.equals (mb2);
    assert ! mb2.equals (mb1);
    assert  mb1.compareTo (mb2) != 0;
    assert  mb2.compareTo (mb1) != 0;

    assert  mb1.equals (mb1);
    assert  mb1.compareTo (mb1) == 0;

    assert  mb2.equals (mb2);
    assert  mb2.compareTo (mb2) == 0;

    assert (check.equals (mb1));
    assert (check.compareTo (mb1) == 0);
    assert (check.hashCode () == mb1.hashCode ());
    assert (mb1.equals (check));
    assert (mb1.compareTo (check) == 0);
    assert (mb1.hashCode () == check.hashCode ());
  }

  @AfterTest
  public void teardownTest ()
  throws NatsException
  {
    ctx.close ();
    ctx = null;
    theConnection.close (); // should not rethrow...
    theConnection = null;
  }
}
