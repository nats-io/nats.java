/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats;

import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 * Various options for setting up the NATS client framework.
 *
 * Follows the go client convention with Java Idioms where it
 * makes sense.
 *
 * Final for now.
 *
 * Most things work here with the exception of tls/ssl changes. Will work those in after regress.
 */
public final class Options
{
  /** The default number of PING messages we wait for responses for until we mark a connection closed */
  public static final int DEFAULT_MAX_PINGS_OUT = 5;
  /** How many pending messages can be sitting on an async subscription before we start throttling delivery
   * of messages and letting the async callback know we have a slow subscriber.
   */
  public static final int DEFAULT_ASYNC_MAX_CHAN_LEN = 65536;
  /** How many pending messages can be on a synchronous subscription before we start throttling delivery
   * of messages and letting the async callback know we have a slow subscriber.
   */
  public static final int DEFAULT_SYNC_MAX_CHAN_LEN = 4;
  /** The maximum number of bytes we will allow in a frame */
  public static final int DEFAULT_MAX_FRAME_SIZE = 1024 * 10;
  /** How often to send pings (in seconds */
  public static final int DEFAULT_PING_INTERVAL = 30;
  /** Default URI we will try to connect to */
  public static final URI DEFAULT_URI;
  /** The URI Scheme we service */
  public static final String NATS_URI_SCHEME = "nats";
  /** Not yet, talk to team about handshake sematics */
  //public static final String NATS_SECURE_URI_SCHEME = "natss";
  /** The default port when none is specified in a NATS scheme'd URI */
  public static final int DEFAULT_PORT = 4222;
  /** The default timeout for I/O operations (connect/read/write/close/etc) */
  public static final long DEFAULT_TIMEOUT_IN_SECONDS = 30;

  static {
    URI def = null;

    try
    {
      def = new URI ("nats://localhost");
    }
    catch (java.net.URISyntaxException ignoreMe)
    {
      // FIXME: make this work the right way, but seriously?
      System.err.println ("nats: code enhancement/startup fixes: " + ignoreMe);
    }
    finally
    {
      DEFAULT_URI = def;
    }
  }

  public Options ()
    {
    }

  public Options (Options other)
    {
      // jam, I hate po java
      this.servers = new java.util.ArrayList<URI> (other.servers);
      this.noRandomize = other.noRandomize;
      this.name = other.name;
      this.verbose = other.verbose;
      this.pedantic = other.pedantic;
      this.secure = other.secure;
      this.allowReconnect = other.allowReconnect;
      this.reconnectWait = other.reconnectWait;
      this.timeout = other.timeout;
      this.closeCB = other.closeCB;
      this.disconnectedCB = other.disconnectedCB;
      this.reconnectedCB = other.reconnectedCB;
      this.asyncErrorCB = other.asyncErrorCB;
      this.pingInterval = other.pingInterval;
      this.maxPingsOut = other.maxPingsOut;
      this.subChanLen = other.subChanLen;
      this.maxFrameSize = other.maxFrameSize;
      this.tracePackets = other.tracePackets;
      this.traceProtocol = other.traceProtocol;
    }

  /** List of servers we will connect to. Connections will randomize, etc so no worries about ownership */
  public java.util.List<URI> servers =  new java.util.ArrayList<URI> ();
  /** Should connections be randomized when we reconnect? */
  public boolean noRandomize = false;
  /** THe name of this client */
  public String name;
  /** Is the protocol verbose? */
  public boolean verbose = false;
  /** Whether we (and the server) check subject, reply, etc. names and other parameters */
  public boolean pedantic = false;
  /** Whether we want a tls / ssl connection or not. Note, this changes in branch #novups to a tls conf. This
   * does not work at this time apart from looking for matches with the server/param checking in the
   * handshake.
   *
   * FIXME: jam: secure options and ssl merge of #novups
   * */
  public boolean secure = false;
  /** Whether we allow reconnects at all if we lose or are unable t oget a connection from one server */
  public boolean allowReconnect = false;
  /** How long to wait when reconnecting to not over throttle the server or have a connect/close storm */
  public long reconnectWait;
  /** How long to wait, in seconds, for connecting and other values when no timeout is specified */
  public long timeout = DEFAULT_TIMEOUT_IN_SECONDS;

  /** Call back for when we get explicitly closed within our process */
  public ConnHandler closeCB;
  /** Call back for when we get disconnected by our peer */
  public ConnHandler disconnectedCB;
  /** Call back for when we get reconnected to another server (or connected the first time) */
  public ConnHandler reconnectedCB;
  /** Call back for when we have an issue with an async message delivery */
  public ErrHandler asyncErrorCB;

  /** How often to ping our peers in seconds */
  public int pingInterval = DEFAULT_PING_INTERVAL;
  /** How many pings without any responses to send before we mark the other side as dead */
  public int maxPingsOut = DEFAULT_MAX_PINGS_OUT;
  /** How long our subscriber channel length is. This is the number of pending messages we allow
   * for any subscription. Once that is reached, messages may be dropped (likely will be). We will
   * also mark the subscriber as slow and call the #asyncErrorCB to notify of that event.
   * Once a new message is delivered to the queue of the subscriber (meaning its started processing),
   * the subscriber state will be reset to not be slow.
   */
  public int subChanLen = DEFAULT_ASYNC_MAX_CHAN_LEN;
  /** The maximum number of bytes we will receive in a NATS message */
  public int maxFrameSize = DEFAULT_MAX_FRAME_SIZE;
  /** Whether raw protocol packet tracing should be on (raw bytes) */
  public boolean tracePackets = false;
  /** Whether NATS protocol tracing should be on (NATS messages */
  public boolean traceProtocol = false;
}
