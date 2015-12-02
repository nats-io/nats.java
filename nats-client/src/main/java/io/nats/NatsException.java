/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats;

/**
 * Represents a NATS-API or infrastructure specific exception.
 * In some cases, underlying exceptions are wrapped in an instance of
 * <i>NATSException</i>.
 */
public class NatsException
  extends Exception
{
  public final static String ErrConnectionClosed = "Connection Closed";
  public final static String ErrSecureConnRequired = "Secure Connection required";
  public final static String ErrSecureConnWanted   = "Secure Connection not available";
  public final static String ErrSecureConnFailed   = "Secure Connection failed";
  public final static String ErrBadSubscription    = "Invalid Subscription";
  public final static String ErrBadSubject         = "Invalid Subject";
  public final static String ErrSlowConsumer       = "Slow Consumer, messages dropped";
  public final static String ErrTimeout            = "Timeout";
  public final static String ErrBadTimeout         = "Timeout Invalid";
  public final static String ErrAuthorization      = "Authorization Failed";
  public final static String ErrNoServers          = "No servers available for connection";
  public final static String ErrJsonParse          = "Connect message, json parse err";
  public final static String ErrChanArg              = "Argument needs to be a channel type";
  public final static String ErrStaleConnection     = "Stale Connection";
  public final static String ErrMaxPayload          = "Maximum Payload Exceeded";
  public final static String ErrMalformedURL         = "Malformed URL";
  public final static String ErrMaxMessagesDelivered = "Maximum messages delivered to subscription";
  public final static String ErrUnknown = "Unknown Exception";

  public NatsException () {}
  public NatsException (String msg) { super(msg); }
  public NatsException (String msg, Throwable nested) { super (msg, nested); }
}
