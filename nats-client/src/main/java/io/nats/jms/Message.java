/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats.jms;

import javax.jms.Destination;
import javax.jms.JMSException;
import java.util.Enumeration;

public class Message
  implements javax.jms.Message
{
  private io.nats.Message underlying;
  private byte correlationID[];
  private long timestamp;
  private String messageID;
  private Destination replyTO;
  private Destination destination;
  private int deliveryMode;
  private boolean redelivered;
  private String type;
  private long expiration;
  private int priority;
  private long deliveryTime;
  private java.util.Map props = new java.util.HashMap ();

  Message (io.nats.Message underlying)
  {
    this.underlying = underlying;
  }
  io.nats.Message getUnderlying ()
  {
    return underlying;
  }
  void setUnderlying (io.nats.Message underlying)
  {
    this.underlying = underlying;
  }
  public String getJMSMessageID () throws JMSException
  {
    return messageID;
  }

  public void setJMSMessageID (String s) throws JMSException
  {
    this.messageID = s;
  }

  public long getJMSTimestamp () throws JMSException
  {
    return timestamp;
  }

  public void setJMSTimestamp (long l) throws JMSException
  {
    this.timestamp = l;
  }

  public byte[] getJMSCorrelationIDAsBytes () throws JMSException
  {
    return correlationID.clone ();
  }

  public void setJMSCorrelationIDAsBytes (byte[] bytes) throws JMSException
  {
    this.correlationID = bytes.clone ();
  }

  public void setJMSCorrelationID (String s) throws JMSException
  {
    setJMSCorrelationIDAsBytes (s.getBytes ());
  }

  public String getJMSCorrelationID () throws JMSException
  {
    return new String (getJMSCorrelationIDAsBytes ());
  }

  public Destination getJMSReplyTo () throws JMSException
  {
    return replyTO;
  }

  public void setJMSReplyTo (Destination destination) throws JMSException
  {
    this.replyTO = destination;
  }

  public Destination getJMSDestination () throws JMSException
  {
    return destination;
  }

  public void setJMSDestination (Destination destination) throws JMSException
  {
    this.destination = destination;
  }

  public int getJMSDeliveryMode () throws JMSException
  {
    return deliveryMode;
  }

  public void setJMSDeliveryMode (int i) throws JMSException
  {
    this.deliveryMode = i;
  }

  public boolean getJMSRedelivered () throws JMSException
  {
    return redelivered;
  }

  public void setJMSRedelivered (boolean b) throws JMSException
  {
    redelivered = b;
  }

  public String getJMSType () throws JMSException
  {
    return type;
  }

  public void setJMSType (String s) throws JMSException
  {
    type = s;
  }

  public long getJMSExpiration () throws JMSException
  {
    return expiration;
  }

  public void setJMSExpiration (long l) throws JMSException
  {
    expiration = l;
  }

  public int getJMSPriority () throws JMSException
  {
    return priority;
  }

  public void setJMSPriority (int i) throws JMSException
  {
    priority = i;
  }

  public void clearProperties () throws JMSException
  {
    props.clear ();
  }

  public boolean propertyExists (String s) throws JMSException
  {
    return props.containsKey (s);
  }

  public boolean getBooleanProperty (String s) throws JMSException
  {
    return (Boolean) props.get (s);
  }

  public byte getByteProperty (String s) throws JMSException
  {
    return (Byte) props.get (s);
  }

  public short getShortProperty (String s) throws JMSException
  {
    return (Short) props.get (s);
  }

  public int getIntProperty (String s) throws JMSException
  {
    return (Integer) props.get (s);
  }

  public long getLongProperty (String s) throws JMSException
  {
    return (Long) props.get (s);
  }

  public float getFloatProperty (String s) throws JMSException
  {
    return (Float) props.get (s);
  }

  public double getDoubleProperty (String s) throws JMSException
  {
    return (Double) props.get (s);
  }

  public String getStringProperty (String s) throws JMSException
  {
    return (String) props.get (s);
  }

  public Object getObjectProperty (String s) throws JMSException
  {
    return props.get (s);
  }

  public Enumeration getPropertyNames () throws JMSException
  {
    return null;
  }

  public void setBooleanProperty (String s, boolean b) throws JMSException
  {
    props.put (s, Boolean.valueOf (b));
  }

  public void setByteProperty (String s, byte b) throws JMSException
  {
    props.put (s, Byte.valueOf (b));

  }

  public void setShortProperty (String s, short i) throws JMSException
  {
    props.put (s, Short.valueOf (i));
  }

  public void setIntProperty (String s, int i) throws JMSException
  {
    props.put (s, Integer.valueOf (i));
  }

  public void setLongProperty (String s, long l) throws JMSException
  {
    props.put (s, Long.valueOf (l));

  }

  public void setFloatProperty (String s, float v) throws JMSException
  {
    props.put (s, Float.valueOf (v));

  }

  public void setDoubleProperty (String s, double v) throws JMSException
  {
    props.put (s, Double.valueOf (v));

  }

  public void setStringProperty (String s, String s1) throws JMSException
  {
    props.put (s, s1);
  }

  public void setObjectProperty (String s, Object o) throws JMSException
  {
    props.put (s, o);
  }

  public void acknowledge () throws JMSException
  {
  }

  public void clearBody () throws JMSException
  {
  }

  public long getJMSDeliveryTime () throws JMSException
  {
    return deliveryTime;
  }

  public void setJMSDeliveryTime (long l) throws JMSException
  {
    deliveryTime = l;
  }

  public <T> T getBody (Class<T> aClass) throws JMSException
  {
    if (aClass == Message.class)
      return (T) this;
    // FIXME: jam: jms conversion logic from oak
    return null;
  }

  public boolean isBodyAssignableTo (Class aClass) throws JMSException
  {
    // FIXME: jam: jms isBodyAssignableTo
    return false;
  }
}
