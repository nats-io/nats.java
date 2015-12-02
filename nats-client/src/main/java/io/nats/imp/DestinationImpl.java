/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats.imp;

import io.nats.Destination;
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

public class DestinationImpl
  implements io.nats.Destination
{
  private final String name;

  public DestinationImpl (String name)
  {
    this.name = name;
    if (name == null)
      {
        throw new NullPointerException ("name");
      }
  }
  public String name ()
  {
    return name;
  }

  public String toString ()
  {
    return String.format ("DestinationImpl<%s>", name);
  }

  public int compareTo (Destination o)
  {
    return (this == o ? 0
            : this.name ().compareTo (o.name ()));
  }

  public boolean equals (Object other)
  {
    return compareTo ((Destination) other) == 0;
  }

  public int hashCode ()
  {
    return name.hashCode ();
  }

  public int length ()
  {
    return name.length ();
  }

  public char charAt (int index)
  {
    return name.charAt (index);
  }

  public CharSequence subSequence (int start, int end)
  {
    return name.subSequence (start, end);
  }

  public void encode (ByteBuf buf)
  {
    buf.writeBytes (name.getBytes (CharsetUtil.US_ASCII));
  }
}
