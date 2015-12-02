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
import io.nats.Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.concurrent.TimeUnit;

public class RequestResponse
{
  static volatile int msgNum = 0;

  public static void main (String args[])
    throws Exception
    {
      try
	{
          final Context ctx = new io.nats.imp.Context ();
          final Connection conn = ctx.connect ();
          final Destination request_subj = conn.destination (args.length >= 1 ? args[0] : "RequestResponseSubject");

	  System.out.println ("connected to nats: " + conn);
          conn.subscribe (request_subj, new MessageHandler<ByteBuf> () {
            final Connection myconn = conn;
            public void onMessage (Subscription subscription,
                                   Destination subject,
                                   Destination reply,
                                   ByteBuf body)
              throws NatsException
            {
              final int value = body.readInt ();
/*
              System.out.println ("replier: sid=" + subscription.sid_as_object
                                  + " subject=" + subject + " reply=" + reply
                                  + " value=" + value);*/
              if (reply == null)
                {
                  System.out.println ("reply is null...");
                  return;
                }
              ByteBuf msg = ByteBufAllocator.DEFAULT.heapBuffer ();
              msg.writeInt (-value);
              myconn.publish (reply, null, msg);
            }
          });
          final int num_to_pub = 10000;
          Thread t = new Thread (new Runnable () {
            public void run ()
            {
              try {
              for (int i = 0; i < num_to_pub; i++)
                {
                  final int expected = i + 1;
                  ByteBuf msg = ByteBufAllocator.DEFAULT.heapBuffer ();
                  msg.writeInt (expected);
    /*              System.out.println ("requestor: sending " + expected
                                      + " on " + request_subj);*/
                  Message<ByteBuf> resp;
                  resp = conn.request (request_subj, msg);
                  assert (resp != null);
                  ByteBuf body = resp.getBody ();
                                    int recv = body.readInt ();
                                    if (-expected != recv)
                                      System.err.println ("oops: bad response: " + recv + " (vs. " + -expected + ")");
                                    msgNum++;
                }
              }catch (NatsException ignore)
              {
              System.err.println ("caught: " + ignore);
                ignore.printStackTrace (System.err);
                System.err.flush ();
              }
            }
          });
          final long done_pub_time = System.currentTimeMillis ();
          t.run ();
          t.join ();
          while (true)
            {
              final int how_many = msgNum;
              final int diff = num_to_pub - how_many;
              final long now = System.currentTimeMillis ();
              final long delta_time = now - done_pub_time;
              //System.out.println ("td=" + delta_time + " diff=" + diff + " received=" + how_many + " sent=" + i);
              //System.out.flush ();
              if (diff == 0 || delta_time > TimeUnit.SECONDS.toMillis (30))
                {
                  System.out.println ("that took: " + TimeUnit.MILLISECONDS.toSeconds (delta_time)
                                      + " to publish " + how_many + " messages of " + 4 + " ("
                                      + (how_many * 4) + ")");
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