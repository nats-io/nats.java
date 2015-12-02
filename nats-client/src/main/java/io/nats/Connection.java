/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats;

import io.netty.buffer.ByteBuf;

/**
 * Represents a logical connection to the NATs message infrastructure.
 */
public interface Connection
{
 void publish (Destination subject, Destination reply, ByteBuf msg)
   throws NatsException;
 Message request (Destination subj,
                  ByteBuf msg,
                  long timeout)
    throws NatsException;
 Message request (Destination subj, ByteBuf msg)
    throws NatsException;
 Subscription subscribe (Destination subject, MessageHandler cb)
    throws NatsException;
 Subscription subscribeSync (Destination subject)
    throws NatsException;
 Subscription queueSubscribe (Destination subject, Destination queue, MessageHandler cb)
   throws NatsException;
 Subscription queueSubscribeSync (Destination subject, Destination queue)
   throws NatsException;
 void close ()
   throws NatsException;
 public Destination destination (String where)
   throws NatsException;
 public Destination newInbox ()
  throws NatsException;
}
