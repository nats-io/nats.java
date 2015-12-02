/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats;

public interface Subscription
{
  Connection getConnection ()
    throws NatsException;
}
