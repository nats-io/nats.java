/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats.examples;

import io.nats.Context;
import io.nats.Destination;
import io.nats.MessageHandler;
import io.nats.Connection;
import io.nats.Subscription;
import io.nats.NatsException;
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

/**
 * Example that looks for every message from nats and prints it out...
 */
public class Sniffer
{
  public static void main (String args[])
    throws Exception
    {
      try
	{
          Context ctx = new io.nats.imp.Context ();
	  Connection conn = ctx.connect ();
	  Destination pattern = conn.destination (">");

          System.out.println ("connected to nats: " + conn);
          conn.subscribe (pattern, new MessageHandler<ByteBuf> () {
            volatile int msgNum = 0;
            public void onMessage (Subscription subscription,
                                   Destination subject,
                                   Destination reply,
                                   ByteBuf body)
              {
                final int n = ++msgNum;
                int body_len = body.readableBytes ();
                int maxB = Math.min (body_len, 16);
                System.out.printf ("received msg #%6s: %s%s (subject=%s, reply=%s)\n",
                                   n,
                                   body.toString (0, maxB, CharsetUtil.US_ASCII),
                                   body_len > maxB ? "..." : "",
                                   subject,
                                   reply);
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
