/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats.imp;

import io.nats.Connection;
import io.nats.Destination;
import io.nats.MessageHandler;
import io.nats.NatsException;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.EventExecutorGroup;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class Subscription
  implements io.nats.Subscription, Runnable
{
  // unique subscription ID (per Connection)
  final SubscriberKey sid;
  // subject representing this subscription
  final Destination subject;
  // optional queue group name
  final Destination queue;
  // number of messages received (not delivered)
  final AtomicLong msgs = new AtomicLong ();
  // number of messages delivered to our call back
  final AtomicLong delivered = new AtomicLong ();
  // number of messages we have dropped (channel full)
  final AtomicLong dropped = new AtomicLong ();
  // number of bytes delivered on this subscription
  final AtomicLong bytes = new AtomicLong ();
  // max number of messages we want to receive before
  // auto unsubscribing, handles races??? why on client ;-)
  final AtomicLong max = new AtomicLong ();
  // reference to the connection that created us
  ClientConnection connection;
  // our call back, final!!
  final MessageHandler mcb;
  // length of our channel (max size of pendingQueue)
  final int chanLen;
  // we always place messages here, up to chanLen will be queued
  final BlockingQueue<Message.MSG> pendingQueue;
  // did we drop any messages? reset once a message is taken.
  boolean slowSubscriber;


  Subscription (SubscriberKey sid,
                Destination subject,
                Destination queue,
		final ClientConnection connection,
		MessageHandler mcb,
                int chanLen)
    {
      this.sid = sid;
      this.subject = subject;
      this.queue = queue;
      this.connection = connection;
      this.mcb = mcb;
      this.chanLen = chanLen;
      if (chanLen <= 0)
        throw new IllegalArgumentException ("chanLen can't be <= 0");
      this.pendingQueue = new ArrayBlockingQueue<Message.MSG> (chanLen);
    }

  /** Are we connected? */
  public boolean isConnected () { return connection != null; }

  /** Unsubscribe from this subscription */
  public void unsubscribe ()
    throws NatsException
  {
    autoUnsubscribe (0);
  }

  /** Unsubscribe from this subscription after receiving max messages */
  public void autoUnsubscribe (int max)
    throws NatsException
  {
    final ClientConnection  c = connection;

    if (c == null)
      throw new NatsException (NatsException.ErrBadSubscription);
    if (max < 0)
      throw new IllegalArgumentException ("max");
    c._unsubscribe (this, max);
  }

  private Message.MSG _nextMSG (long timeout)
    throws Exception
  {
    ClientConnection c;
    long m, del;

    synchronized (this)
      {
        m = max.get ();
        c = connection;

        if (c == null)
          throw new NatsException (NatsException.ErrBadSubscription);

        if (slowSubscriber)
          {
            slowSubscriber = false;
            throw new NatsException (NatsException.ErrSlowConsumer);
          }
      }
    final Message.MSG msg = pendingQueue.poll (timeout, TimeUnit.MILLISECONDS);
    if (msg != null)
      {
        del = delivered.incrementAndGet ();

        if (m > 0)
          {
            if (del > m)
              throw new NatsException (NatsException.ErrMaxMessagesDelivered);
            if (del == m)
              {
                // FIXME: jam: gatekeeper check
                c._close (this);;
              }
          }
      }
    return msg;
  }

  public Message.MSG nextMsg (long timeout)
    throws Exception
  {
    if (mcb != null)
      throw new IllegalStateException ("can't call sync call on async subscriber");
    final Message.MSG res = _nextMSG (timeout);
    return res;
  }

  public void run ()
  {
    try
      {
        if (mcb == null)
          throw new IllegalStateException ("can't call sync call on async subscriber");
        final Message.MSG msg = _nextMSG (-1);
        if (msg != null)
          {
            mcb.onMessage (this, msg.getSubject (), msg.getReply (), msg.getBody ());
          }
      }
    catch (NatsException ex)
      {
        // blah
      }
    catch (Exception ex)
      {
        // blue
      }
  }

  void msgReceived (final ChannelHandlerContext ctx,
                    final ClientConnection connection,
                    final Message.MSG msg,
                    final EventExecutorGroup execGroup)
    throws NatsException
  {
    ClientConnection c = connection;

    if (c == null)
      return;

    long m = max.get ();
    long recv = msgs.get ();
    if (m > 0 && recv > m)
      {
        dropped.incrementAndGet ();
        c._close (this);
        return;
      }
    msgs.incrementAndGet ();
    bytes.addAndGet (msg.getBody ().readableBytes ());
    if (! pendingQueue.offer (msg))
      {
        dropped.incrementAndGet ();
        c.processSlowSubscriber (this);
      }
    else
      {
        slowSubscriber = false;
        if (mcb != null)
          ctx.fireChannelRead (this);
      }
  }

  public Connection getConnection ()
    throws NatsException
  {
    Connection res = connection;
    if (res == null)
      throw new NatsException (NatsException.ErrBadSubscription);
    return res;
  }
}
