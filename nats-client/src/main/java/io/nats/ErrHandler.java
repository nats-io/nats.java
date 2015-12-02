/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats;

public interface ErrHandler
{
  void callback (Connection conn,
                 Subscription sub,
                 NatsException cause);
}
