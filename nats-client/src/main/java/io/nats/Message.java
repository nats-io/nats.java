/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats;

public interface Message<T>
{
  Destination getSubject ();
  Destination getReply ();
  T getBody ();
}
