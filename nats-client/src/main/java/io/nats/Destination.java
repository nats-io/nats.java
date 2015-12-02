/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats;

/**
 * Interface which represents a message end point
 */
public interface Destination
  extends Comparable<Destination>, CharSequence
{
  public String name ();
}
