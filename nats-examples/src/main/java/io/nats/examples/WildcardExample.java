/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats.examples;

import io.nats.Connection;
import io.nats.Context;
import io.nats.Destination;
import io.nats.MessageHandler;
import io.nats.NatsException;
import io.nats.Subscription;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.CharsetUtil;
import sun.nio.cs.US_ASCII;

import java.util.concurrent.TimeUnit;

/* Simple publishing example */
public class WildcardExample
{
  private static volatile int msgNum = 0;
  private static final String SUBSCRIBE_SUBJECT = "/device/>";
  private static final String SUBSCRIBE_SUBJECT_FORMAT = "/device/%08X/connection";

  public static void main (String args[])
    throws Exception
    {
      try
	{
          boolean subscribe = true;
          Context ctx = new io.nats.imp.Context ();
          Connection conn = ctx.connect ();
          final Destination subject = conn.destination (SUBSCRIBE_SUBJECT);
          final ByteBuf msg_body = ByteBufAllocator.DEFAULT.heapBuffer ();

          msg_body.writeBytes ("something".getBytes (CharsetUtil.US_ASCII));
          msg_body.readerIndex (0);
          final int MSG_SIZE = msg_body.readableBytes ();

	  System.out.println ("connected to nats: " + conn);

          if (subscribe)
            {
              conn.subscribe (subject, new MessageHandler<ByteBuf> () {

              public void onMessage (Subscription subscription,
                                     Destination subject,
                                     Destination reply,
                                     ByteBuf body)
              {
                final int n = ++msgNum;

                System.out.printf ("%5s: received message on %s: %s\n",
                                   n, subject, body.toString (CharsetUtil.US_ASCII));
                System.out.flush ();
              }});
            }

          int i;
          for (i = 0; i < 10; i++)
            {
              Destination dest = conn.destination (String.format (SUBSCRIBE_SUBJECT_FORMAT, i));
              conn.publish (dest, null, msg_body.duplicate ());
            }
          if (!subscribe)
            {
              System.out.println ("published " + i + " messages to " + subject);
              return;
            }
          final long done_pub_time = System.currentTimeMillis ();
          while (true)
            {
              final int how_many = msgNum;
              final int diff = i - how_many;
              final long now = System.currentTimeMillis ();
              final long delta_time = now - done_pub_time;
              //System.out.println ("td=" + delta_time + " diff=" + diff + " received=" + how_many + " sent=" + i);
              //System.out.flush ();
              if (diff == 0)
                {
                  System.out.println ("that took: " + TimeUnit.MILLISECONDS.toSeconds (delta_time)
                                      + " to publish " + i + " messages of " + msg_body.writerIndex () + " ("
                                      + (i * msg_body.writerIndex ()) + ")");
                  break;
                }
              Thread.sleep (TimeUnit.SECONDS.toMillis (1) / 200);
            }
        }
      catch (NatsException ex)
	{
	  System.err.println ("caught nats exception: " + ex);
	  ex.printStackTrace (System.err);
	  System.exit (1);
	}
    }
}