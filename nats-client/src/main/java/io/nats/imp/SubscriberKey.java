/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats.imp;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

/**
 * Optimize the creation and comparison/hashing/etc of subscription IDs/keys...
 */
public abstract class SubscriberKey<T extends SubscriberKey>
{
  private final byte rawBytes[];

  protected SubscriberKey (byte rawBytes[])
  {
    this.rawBytes = rawBytes;
  }
  abstract public Object value ();

  byte[] rawBytes () { return rawBytes; }

  public static class ObjectKey extends SubscriberKey<ObjectKey>
  {
    private final int hashCode;
    private final Object value;

    public ObjectKey (Object value)
    {
      super (value.toString ().getBytes (CharsetUtil.US_ASCII));
      this.value = value;
      this.hashCode = value.hashCode ();
    }
    public int hashCode ()
    {
      return hashCode;
    }
    public Object value ()
    {
      return value;
    }
  }

  public static class LongKey extends SubscriberKey<LongKey>
  {
    private final int hashCode;
    private final long value;
    private Long objectValue;

    public LongKey (long value)
    {
      super (Long.valueOf (value).toString ().getBytes (CharsetUtil.US_ASCII));
      this.value = value;
      // JDK 1.8 code...
      // this.hashCode = Long.hashCode (value);
      // JDK 1.7 code...
      this.hashCode = (int) ((value >>> 32) ^ value);
    }

    public LongKey (Long value)
    {
      this (value.longValue ());
      this.objectValue = value;
    }

    public int hashCode ()
    {
      return hashCode;
    }

    public boolean equals (Object other)
    {
      return (this == other
              || (other instanceof LongKey
                  && ((LongKey) other).value == value));
    }

    public Object value ()
    {
      return (objectValue == null
              ? (objectValue = Long.valueOf (value))
              : objectValue);
    }
  }

  public static SubscriberKey newInstance (ByteBuf buf)
  {
    return new ObjectKey (buf.toString (CharsetUtil.US_ASCII));
  }

  public static SubscriberKey newInstance (long sid)
  {
    return new LongKey (sid);
  }
}
