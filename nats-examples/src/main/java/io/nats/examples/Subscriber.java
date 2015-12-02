/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats.examples;

import io.nats.Context;
import io.nats.Connection;
import io.nats.Destination;
import io.nats.MessageHandler;
import io.nats.NatsException;
import io.nats.Subscription;
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

/* Simple subscriber example */
public class Subscriber
{
  static volatile int msgNum = 0;

  public static void main (String args[])
    throws Exception
    {
      try
	{
          Context ctx = new io.nats.imp.Context ();
          Connection conn = ctx.connect ();
          final Destination subject = conn.destination (args.length >= 1 ? args[0] : "PublisherSubject");
          final String body_string = args.length >= 2 ? args[1] : "Hello world";

          conn.subscribe (subject, new MessageHandler<ByteBuf> () {

            public void onMessage (Subscription subscription,
                                   Destination subject,
                                   Destination reply,
                                   ByteBuf body)
            {
              final int n = ++msgNum;
              int body_len = body.readableBytes ();
              int maxB = Math.min (body_len, 16);
              System.out.printf ("received msg #%6s: %s%s\n",
                                 n,
                                 body.toString (0, maxB, CharsetUtil.US_ASCII),
                                 body_len > maxB ? "..." : "");
              System.out.flush ();
            }
          });
        }
      catch (NatsException ex)
	{
	  System.err.println ("caught nats exception: " + ex);
	  ex.printStackTrace (System.err);
	  System.exit (1);
	}
    }
}