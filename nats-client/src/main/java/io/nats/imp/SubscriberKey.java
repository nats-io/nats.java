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
  @Override
  abstract public boolean equals (Object other);
  @Override
  abstract public int hashCode ();

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

    public boolean equals (Object other)
    {
      return (this == other
              || (other instanceof ObjectKey
                  && value.equals (((ObjectKey) other).value)));
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
                  && value == ((LongKey) other).value));
    }

    public Object value ()
    {
      return (objectValue == null
              // FIXME: jam: rethink all this, we should hash the bytes and be done with it rather than
              // FIXME: jam: having seperate subclasses, we shouldn't create objects till required ideally... hmmm
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

  public String toString ()
  {
    return value ().toString ();
  }
}
