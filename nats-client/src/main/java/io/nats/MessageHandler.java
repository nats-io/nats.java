/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats;

public interface MessageHandler<T>
{
  void onMessage (Subscription subscription,
                  Destination subject,
                  Destination reply,
                  T body)
  throws NatsException;
}
