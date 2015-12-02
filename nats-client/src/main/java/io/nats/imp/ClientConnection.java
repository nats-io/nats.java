/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats.imp;

import io.nats.Context;
import io.nats.Destination;
import io.nats.ErrHandler;
import io.nats.MessageHandler;
import io.nats.NatsException;
import io.nats.Options;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPipelineException;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.concurrent.EventExecutorGroup;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class ClientConnection
  implements io.nats.Connection
{
  private final static boolean DEBUG = false;
  private final static int SRV_POOL_SIZE = 16;
  private final static String INBOX_PREFIX = "_INBOX.";
  private static final int NUM_THREADS = Runtime.getRuntime ().availableProcessors () * 4;


  private static final String SERVER_ID_KEY = "server_id";
  private static final String TLS_REQUIRED_KEY = "tls_required";

  /** Overall connection status */
  private enum Status
  {
    /** Indicates we are closed, transition to CONNECTING, CONNECTED */
    CLOSED,
    /** Indicates we are connected and have a valid handshake/etc., transition to DISCONNECTED/CLOSED */
    CONNECTED,
    /** Indicates the far end has disconnected us for some reason, server wise or infrastructure
     * Transition to same states as CLOSED
     */
    DISCONNECTED,
    /** Indicates we are re-connecting, must have been connected or DISCONNECTED */
    RECONNECTING,
    /** Indicates we are trying to connect, after the first time... */
    CONNECTING,
    /** Indicates we are awaiting the handshake to complete (or timeout)... */
    AWAITING_HANDSHAKE
  }

  /** Information about a specific end point, follows the go client */
  private final static class Srv
  {
    final URI uri;
    // Did we ever connect and handshake?
    boolean didConnect;
    // How many times?
    int reconnects;
    // the last time we tried
    long lastAttempt;

    Srv (URI uri) {
      this.uri = uri;
      assert (uri != null);
      assert (uri.getScheme ().equals (Options.NATS_URI_SCHEME));
    }
    public String toString () {
      return ("Srv{"
              + ", uri: " + uri
              + ", didConnect: " + didConnect
              + ", reconnects: " + reconnects
              + ", lastAttempt: " + lastAttempt
              + "}");
    }
  }

  private final Statistics stats = new Statistics ();
  final Options opts;
  private Srv currentServer;
  private EventLoopGroup eventLoopGroup;
  private EventExecutorGroup execGroup;
  private Channel conn;

  private final ArrayList<Srv> srvPool = new ArrayList<Srv> (SRV_POOL_SIZE);
  private final java.util.Map<SubscriberKey,Subscription> subs = new ConcurrentHashMap<SubscriberKey, Subscription> ();
  private Status status = Status.CONNECTING;
  private NatsException error;
  private final List<Message> pending_messages = new ArrayList<Message> ();
  private java.util.Map serverInfo;
  private final Object stateLock = new Object ();

  private final static AtomicLong _sid = new AtomicLong ();
  private static long getNextSIDValue () { return _sid.getAndIncrement (); }

  public ClientConnection (Context context, Options opts)
    throws NatsException
    {
      this.opts = new Options (opts);
    }

  void processSlowSubscriber (final Subscription sub)
    throws NatsException
  {
    error = new NatsException (NatsException.ErrSlowConsumer);
    eventLoopGroup.execute (new Runnable () {
      public void run () {
        final ErrHandler handler = ClientConnection.this.opts.asyncErrorCB;
        if (handler != null && sub.slowSubscriber == false)
          handler.callback (ClientConnection.this, sub, error);
      }
    });
  }

  /** Pick the next valid server in the pool */
  private  void _pickNextServer (List newPool)
    throws NatsException
    {
      srvPool.clear ();
      srvPool.addAll (newPool);

      this.currentServer = null;
      this.error = null;

      if (srvPool.size () == 0)
        throw new NatsException (NatsException.ErrNoServers);
      currentServer = srvPool.get (0);
      assert (currentServer != null);
    }

  private void _send (ChannelHandlerContext ctx, Message mesg)
    throws NatsException
    {
      /*if (ctx != null && ctx.executor ().inEventLoop ())
        connection.write (mesg);
      else
        {*/
          if (conn != null) // if (conn != null && _isConnected ())
            conn.writeAndFlush (mesg);
        /*}*/
    }

  private void _connect ()
    throws NatsException
    {
      if (opts.timeout < 0)
        throw new NatsException (NatsException.ErrBadTimeout);
      if (currentServer == null)
        throw new NatsException (NatsException.ErrNoServers);
      status = Status.AWAITING_HANDSHAKE;
      currentServer.lastAttempt = System.currentTimeMillis ();
      currentServer.reconnects++;
      ClientHandshakeHandler handshake = new ClientHandshakeHandler (eventLoopGroup, eventLoopGroup,
                                                                     currentServer.uri, opts,
                                                                     this);

      handshake.connect ();
    }

  void handshakeCompleted (boolean success,
                           Channel channel,
                           java.util.Map serverInfo,
                           Throwable cause)
  {
    if (success)
      {
        this.serverInfo = serverInfo;
        this.conn = channel;

        ChannelPipeline pipe = channel.pipeline ();
        /*
        if (opts.tracePackets || DEBUG)
          pipe.addLast ("frame tracer", new LoggingHandler(LogLevel.INFO));
        pipe.addLast ("peer-watcher", new IdleStateHandler (0, 0, opts.pingInterval));
        pipe.addLast ("decoder", new NatsMessageDecoder (opts.maxFrameSize, true));
        pipe.addLast ("encoder", new MessageEncoder ());
        if (opts.traceProtocol || DEBUG)
          pipe.addLast ("protocol-tracer", new LoggingHandler(LogLevel.INFO));*/
        pipe.addLast ("protocol-handler", new NatsProtocolHandler ());
        pipe.addLast (execGroup, "message-handler", new NatsClientHandler ());
        currentServer.didConnect = true;
        currentServer.reconnects = 0;
      }
    else
      {
        error = (cause instanceof NatsException
                 ? (NatsException) cause
                 : new NatsException ("handshake failure", cause));
        currentServer.didConnect = false;
        currentServer = null;
        /*
        try
          {
            _connect ();
          }
        _close (Status.DISCONNECTED, false);*/
      }

    synchronized (stateLock)
      {
        stateLock.notifyAll ();
      }
  }
  private class NatsProtocolHandler
    extends ChannelDuplexHandler
  {
    private int expected_pongs;

    final static private int
      CONNECTED = 0,
      ERROR = 1,
      CLOSED = -1;

    private int state;

    private NatsProtocolHandler (boolean forServer)
      {
        this.state = CONNECTED;
      }
    private NatsProtocolHandler ()
    {
      this (false);
    }
    public void handlerRemoved (ChannelHandlerContext ctx)
      throws NatsException
      {
        if (state != CLOSED)
          {
            state = CLOSED;
            _close (Status.DISCONNECTED, true);
            _connect ();
          }
      }

    @Override
    public void channelReadComplete (ChannelHandlerContext ctx)
    {
      ctx.flush ();
    }

    protected void pingReceived (ChannelHandlerContext ctx)
      throws NatsException
    {
      _send (ctx, Message.PONG);
    }

    protected void pongReceived (ChannelHandlerContext ctx)
      throws NatsException
    {
      if (expected_pongs == 0)
        {
          // Ignore it...
        }
      else
        expected_pongs--;
    }

    protected void msgReceived (ChannelHandlerContext ctx, Message.MSG msg)
      throws NatsException
    {
      final Subscription sub = subs.get (msg.sid);
      if (sub != null)
        sub.msgReceived (ctx, ClientConnection.this, msg, execGroup);
    }

    public void exceptionCaught (ChannelHandlerContext ctx, Throwable cause)
    {
      final ErrHandler handler = opts.asyncErrorCB;
      if (handler != null)
        {
          NatsException ex;
          if (cause instanceof NatsException)
            ex = (NatsException) cause;
          else
            ex = new NatsException (NatsException.ErrUnknown, cause);
          handler.callback (ClientConnection.this, null, ex);
        }
      else
        {
          System.err.println ("nats: No error callback: caught exception on " + this
                              + ": " + cause);
          cause.printStackTrace (System.err);
          System.err.flush ();
        }
    }

    protected void errReceived (ChannelHandlerContext ctx, String msg)
      throws NatsException
    {
      exceptionCaught (ctx, new NatsException (msg));
    }

    protected void okReceived (ChannelHandlerContext ctx)
      throws NatsException
    {
    }

    protected void subReceived (ChannelHandlerContext ctx, Message msg)
    throws NatsException
    {
    }
    protected void pubReceived (ChannelHandlerContext ctx, Message msg)
    throws NatsException
    {
    }

    protected void unsubReceived (ChannelHandlerContext ctx, Message msg)
    throws NatsException
    {
    }

    @Override
    public void userEventTriggered (ChannelHandlerContext ctx, Object evt)
      throws Exception
    {
      // Called after we have some inactivity
      // send a ping...
      if (evt instanceof IdleStateEvent)
        {
          IdleStateEvent e = (IdleStateEvent) evt;
          if (e.state () == IdleState.ALL_IDLE)
            {
              if (expected_pongs >= opts.maxPingsOut)
                {
                  ctx.fireExceptionCaught (new NatsException (NatsException.ErrStaleConnection));
                  state = ERROR;
                }
              _send (ctx, Message.PING);
              expected_pongs++;
            }
        }
      else
        super.userEventTriggered (ctx, evt);
    }

    @Override
    public void channelRead (ChannelHandlerContext ctx, Object frame)
      throws NatsException
    {
      if (state == CLOSED
          || state == ERROR
          || frame instanceof Message == false)
        return;

      Message msg = (Message) frame;
      stats._updateMsgs (1);

      Message.Verb t = msg.type ();
      // Our state machine allows OK/-ERR before CONNECT/INFO, oh well...
      if (t == Message.Verb.OK)
        {
          okReceived (ctx);
          return;
        }
      else if (t == Message.Verb.ERR)
        {
          errReceived (ctx, ((Message.ERR) msg).message);
          return;
        }

      switch (state)
      {
      case CONNECTED:
        if (t == Message.Verb.PING)
          pingReceived (ctx);
        else if (t == Message.Verb.PONG)
          pongReceived (ctx);
        else if (t == Message.Verb.MSG)
          msgReceived (ctx, (Message.MSG) msg);
        else if (t == Message.Verb.SUB)
          subReceived (ctx, msg);
        else if (t == Message.Verb.UNSUB)
          unsubReceived (ctx, msg);
        else if (t == Message.Verb.PUB)
          pubReceived (ctx, msg);
        else
          break; // we have an error;
        return;
      }
      state = ERROR;
      throw new NatsException ("Unexpected message for state=" + state + " msg="+ msg);
    }
  }

  private class NatsClientHandler
  extends ChannelInboundHandlerAdapter
  {
    @Override
    public void channelRead (ChannelHandlerContext ctx, Object msg)
    {
      if (msg instanceof Subscription)
        {
          Subscription s = (Subscription) msg;
          s.run ();
        }
    }
  }

  private void _close (Status status, boolean doCBs)
    throws NatsException
    {
      if (_isClosed ())
        {
          this.status = status;
          return;
        }
      this.status = Status.CLOSED;

      for (java.util.Map.Entry<?, Subscription> entry : subs.entrySet ())
        {
          Subscription s = entry.getValue ();
          if (s.mcb == null)
            _unsubscribe (s, 0);
          assert (s.connection == null);
        }
      subs.clear ();

      if (doCBs && conn != null && opts.disconnectedCB != null)
        {
          eventLoopGroup.execute (new Runnable () {
              public void run () {
                opts.disconnectedCB.callback (ClientConnection.this);
              }});
        }
      if (doCBs && opts.closeCB != null)
        {
          eventLoopGroup.execute (new Runnable () {
            public void run () {
              opts.closeCB.callback (ClientConnection.this);
            }});
        }

      this.status = status;
      if (conn != null)
        {
          conn.close ();
          conn = null;
        }

    }

  public void _setupAndRun ()
    throws NatsException
  {
    // Copy of the URLs...
    ArrayList pool = new ArrayList ();
    for (URI uri : opts.servers)
      pool.add (new Srv (uri));
    if (opts.noRandomize == false)
      Collections.shuffle (pool);
    if (pool.size () == 0 && Options.DEFAULT_URI != null)
      pool.add (new Srv (Options.DEFAULT_URI));
    _pickNextServer (pool);
    _connect ();
    synchronized (stateLock)
      {
        boolean done = false;

        try
          {
            stateLock.wait ();
            done = conn != null;
          }
        catch (InterruptedException ie)
          {
            Thread.interrupted ();
          }
        if (error != null)
          throw error;
        if (!done)
          throw new NatsException (NatsException.ErrNoServers);
      }
  }

  public URI connectedURI ()
  {
    final URI res = (status == Status.CONNECTED
                     ? currentServer.uri
                     : null);
    return res;
  }

  public String connectedServerID ()
  {
    final String res = ((status == Status.CONNECTED
                        ? serverInfo.get (SERVER_ID_KEY)
                        : null)).toString ();
    return res;
  }

  public Throwable lastError ()
  {
    return error;
  }

  private static boolean
  get_boolean_value (Object v)
  {
    if (v instanceof Boolean == false)
      {
        if (v instanceof String)
          v = Boolean.valueOf ((String) v);
      }
    return v == Boolean.TRUE;
  }

  private void _processOpErr (String err)
    throws NatsException
  {
    if (_isConnecting () || _isClosed () || _isReconnecting ())
      return;

    if (opts.allowReconnect && status == Status.CONNECTED)
      {
        status = Status.RECONNECTING;
      }
    else
      {
        Object val;

        val =
        status = Status.DISCONNECTED;
        if (error != null)
          ;
        else if (serverInfo != null
                 && get_boolean_value (serverInfo.get (TLS_REQUIRED_KEY))) // FIXME: jam: not sure this is right,
          this.error = new NatsException (NatsException.ErrSecureConnRequired);
        else
          this.error = new NatsException (NatsException.ErrConnectionClosed);
        this.error = new NatsException (err, this.error);
        close ();
      }
  }

  private void _processErr (String error)
    throws NatsException
  {
    if (NatsException.ErrStaleConnection.equals (error)
        || NatsException.ErrStaleConnection.substring (5).equals (error))
      _processOpErr (error);
    else
      {
        boolean doCBs;
        doCBs = (status != Status.CONNECTING);
        this.error = new NatsException (error);
        _close (Status.CLOSED, doCBs);
      }
  }

  private static void _check_subject (boolean required, Destination dest)
    throws NatsException
  {
    String name = dest != null ? dest.name () : null;

    if ((required &&  (name == null || name.length () == 0))
        || (name != null
            && (name.indexOf (' ') != -1
                || name.indexOf ('\t') != -1
                || name.indexOf ('\r') != -1
                || name.indexOf ('\n') != -1)))
      throw new NatsException (NatsException.ErrBadSubject + ": " + name);
  }

  private void _write_mesg (Message mesg)
  throws NatsException
  {
    if (_isReconnecting ()
        && mesg instanceof Message.MsgPub)
      pending_messages.add (mesg);
    else
      {
        _send (null, mesg);
      }
  }

  public void publish (Destination subject, Destination reply, ByteBuf msg)
    throws NatsException
  {
    final int body_size = msg.readableBytes ();

    if (subject == null || subject.length () == 0)
      throw new NatsException (NatsException.ErrBadSubject);
    if (opts.pedantic)
      {
        _check_subject (true, subject);
        _check_subject (false, reply);
      }
    final Message.PUB pub = new Message.PUB (subject, reply, msg);
    if (body_size > opts.maxFrameSize)
      throw new NatsException (NatsException.ErrMaxPayload);
    if (_isClosed ())
      throw new NatsException (NatsException.ErrConnectionClosed);
    if (error != null)
      throw error;
    _write_mesg (pub);
  }

  public io.nats.Message request (Destination subj,
                                  ByteBuf msg,
                                  long timeout)
    throws NatsException
  {
      final long sid_value = getNextSIDValue ();
      final Destination inbox = newInbox (sid_value);
      final Subscription sub = _subscribe (inbox, null, sid_value, null,
                                           Options.DEFAULT_SYNC_MAX_CHAN_LEN);
      try
        {
          sub.autoUnsubscribe (1);
          publish (subj, inbox, msg);
          return sub.nextMsg (timeout);
        }
      catch (InterruptedException ex)
        {
          Thread.interrupted ();
          return null;
        }
      catch (NatsException ne)
        {
          throw ne;
        }
      catch (Exception ex)
        {
          throw new NatsException (NatsException.ErrUnknown, ex);
        }
      finally
        {
          // FIXME: jam: races on autounsubscribe
// hmmm          sub.unsubscribe ();
        }
  }

  public io.nats.Message request (Destination subj, ByteBuf msg)
  throws NatsException
  {
    return request (subj, msg, TimeUnit.SECONDS.toMillis (opts.timeout));
  }

  /* public for testing */ public Destination newInbox (long sid)
  {
    StringBuffer buff = new StringBuffer (INBOX_PREFIX);
    buff.append (sid);
    buff.append ('.');
    buff.append (System.nanoTime ());
    // FIXME: jam: not globally unique... is it?
    // FIXME: probably so, but... maybe start sids on random
    // or spin 16 randoms/cipher for both?
    final String res = buff.toString ();
    // anyway....
    return new DestinationImpl (res);
  }

  public Destination newInbox ()
  {
    return newInbox (getNextSIDValue ());
  }

  private Subscription _subscribe (Destination subject, Destination queue,
                                   long sid_value,
                                   MessageHandler cb, int chanLen)
    throws NatsException
  {
    if (_isClosed ())
      throw new NatsException (NatsException.ErrConnectionClosed);

    final SubscriberKey sid = SubscriberKey.newInstance (sid_value);
    Subscription sub = new Subscription (sid, subject, queue, this, cb,
                                         chanLen <= 0 ? opts.subChanLen : chanLen);
    Subscription prev = subs.put (sub.sid, sub);
    assert (prev == null);
    _write_mesg (new Message.SUB (subject, sid, queue));
    return sub;
  }

  public Subscription subscribe (Destination subject, MessageHandler cb)
    throws NatsException
  {
    return _subscribe (subject, null, getNextSIDValue (), cb, -1);
  }

  public Subscription subscribeSync (Destination subject)
    throws NatsException
  {
    return subscribe (subject, null);
  }

  public Subscription queueSubscribe (Destination subject, Destination queue, MessageHandler cb)
   throws NatsException
  {
    return _subscribe (subject, queue, getNextSIDValue (), cb, -1);
  }

  public Subscription queueSubscribeSync (Destination subject, Destination queue)
    throws NatsException
  {
    return queueSubscribe (subject, queue, null);
  }

  void _unsubscribe (Subscription sub, int max)
    throws NatsException
  {
    if (_isClosed ())
      throw new NatsException (NatsException.ErrConnectionClosed);

    Subscription found = subs.get (sub.sid);
    if (found == null) // Already unsubscribed
      return;
    assert (found == sub);
    // we hold the lock for subscription mods here!!!
    // lor at its best ;-)
    found.max.set (max);
    if (max <= 0)
      {
        // Remove it now!
         _close (sub);
      }
    if (! _isReconnecting ())
      _write_mesg (new Message.UNSUB (found.subject, max));
  }

  void _close (Subscription sub)
  {
    Subscription found = subs.remove (sub.sid);
    assert (found == sub);
    assert (sub.connection == this);
    sub.connection = null;
    sub.max.set (sub.delivered.get ());
    sub.pendingQueue.clear ();
  }

  public void close ()
    throws NatsException
  {
    _close (Status.CLOSED, true);;
  }
  private boolean _isReconnecting () { return status == Status.RECONNECTING; }
  private boolean _isConnecting () { return status == Status.RECONNECTING; }
  private boolean _isClosed () { return status == Status.CLOSED; }
  private boolean _isConnected () { return status == Status.CONNECTED; }


  public Destination destination (String where)
  throws NatsException
  {
    return new DestinationImpl (where);
  }
}
