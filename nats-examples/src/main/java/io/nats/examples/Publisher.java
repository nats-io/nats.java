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
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.CharsetUtil;

import java.util.concurrent.TimeUnit;

/* Simple publishing example */
public class Publisher
{
  static volatile int msgNum = 0;

  public static void main (String args[])
    throws Exception
    {
      try
	{
          boolean subscribe = true;
          Context ctx = new io.nats.imp.Context ();
          Connection conn = ctx.connect ();
          final Destination subject = conn.destination (args.length >= 1 ? args[0] : "PublisherSubject");
          final String body_string = args.length >= 2 ? args[1] : "Hello world";
          final ByteBuf msg_body = ByteBufAllocator.DEFAULT.heapBuffer ();

          msg_body.writeBytes (body_string.getBytes (CharsetUtil.US_ASCII));
          { for (int i = msg_body.writerIndex (); i < 1024; i++)
            msg_body.writeByte ('#');
          msg_body.readerIndex (0); }
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
                  assert (body.readableBytes () == MSG_SIZE);
                //try { Thread.sleep (100);}catch(Exception ignore){}
              }});
            }

          int i;
          for (i = 0; i < 10000; i++)
            {
              conn.publish (subject, null, msg_body.duplicate ());
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