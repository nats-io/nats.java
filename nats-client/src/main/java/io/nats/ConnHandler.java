/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats;

public interface ConnHandler
{
  void callback (Connection conn);
}
