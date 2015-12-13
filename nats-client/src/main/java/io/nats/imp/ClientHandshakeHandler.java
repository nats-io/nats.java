/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats.imp;

import io.nats.NatsException;
import io.nats.Options;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.ScheduledFuture;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ClientHandshakeHandler
  extends ChannelInboundHandlerAdapter
  implements FutureListener
{
  private final static String VERSION = "0.1.3";
  private final static String LANGUAGE = "nats-jnats-camros";
  private static boolean DEBUG = false;
  private EventLoopGroup workerGroup;
  private EventExecutorGroup eventExecutorGroup;
  private final URI uri;
  private final Options options;
  private long startTime;
  private long deadline;
  private Throwable failureCause;
  private Channel channel;
  private SslContext sslContext;
  private SslHandler sslHandler;
  private Future sslHandshakeFuture;
  private Message.CONNECT connectMessage;
  private ClientConnection handler;
  private Throwable error;

  enum States
  {
    START,
    CONNECTING,
    CONNECTED,
    CONNECT_FAILURE,
    CONNECT_CANCELLED,
    CONNECT_TIMEOUT,
    CONNECT_ERR,
    AWAITING_SSL_HANDSHAKE,
    AWAITING_ACK,
    HANDSHAKE_COMPLETE,
  }
  private States state;

  private boolean done ()
  {
    return (state == States.CONNECT_FAILURE
            || state == States.CONNECT_CANCELLED
            || state == States.CONNECT_TIMEOUT
            || state == States.CONNECT_ERR
            || state == States.HANDSHAKE_COMPLETE);

  }

  /** Helper inner class to represent semantics about the NATS INFO/CONNECT parameters */
  private static abstract class Info extends java.util.HashMap<String,String>
  {
    // client or server version
    String version;
    boolean tls_required;
    protected static final String keys[] = {"version", "tls_required" };
    protected static List<String> combine (List<String>... v)
    {
      List<String> res = new ArrayList<String> ();
      for (List<String> el : v)
        res.addAll (el);
      return res;
    }
    protected String get (String k, Object defaultV)
    {
      String val = get (k);
      if (val == null && defaultV != null)
        val = defaultV.toString ();
      return val;
    }
    protected void updateCached ()
    {
      version = get (keys[0], null);
      tls_required = Boolean.valueOf (get (keys[1], false));
    }
    protected void putCached ()
    {
      put (keys[0], version);
      put (keys[1], Boolean.valueOf (tls_required).toString ());
    }
  }

  /** Helper inner class to represent semantics about the NATS INFO parameters */
  private static final class ServerInfo extends Info
  {
    String server_id;
    String host;
    int port;
    String version;
    boolean auth_required;
    long max_payload;

    private final static String myKeys [] = { "server_id", "host", "port", "auth_required", "max_payload" };
    private final static List keyList = combine (Arrays.asList (keys), Arrays.asList (myKeys));
    protected void updateCached ()
    {
      super.updateCached ();
      server_id = get (myKeys[0], null);
      host = get (myKeys[1]);
      port = Integer.valueOf (get (myKeys[2], "0"));
      auth_required = Boolean.valueOf (get (myKeys[3], false));
      max_payload = Long.valueOf (get (myKeys[4], Options.DEFAULT_MAX_FRAME_SIZE));
    }
    protected void putCached ()
    {
      super.putCached ();
      put (myKeys[0], server_id);
      put (myKeys[1], host);
      put (myKeys[2], Integer.valueOf (port).toString ());
      put (myKeys[3], Boolean.valueOf (auth_required).toString ());
      put (myKeys[4], Long.valueOf (max_payload).toString ());

    }
  }

  /** Helper inner class to represent semantics about the NATS CONNECT paramters */
  static final class ClientInfo extends Info
  {
    boolean verbose;
    boolean pedantic;
    String user;
    String pass;
    String name;
    String lang;

    private final static String myKeys [] = { "verbose", "pedantic", "user", "pass", "name", "lang" };
    private final static List keyList = combine (Arrays.asList (keys), Arrays.asList (myKeys));
    @Override
    protected void updateCached ()
    {
      super.updateCached ();
      verbose = Boolean.valueOf (get (myKeys[0], Boolean.FALSE));
      pedantic = Boolean.valueOf (get (myKeys[1], Boolean.FALSE));
      user = get (myKeys[2]);
      pass = get (myKeys[3]);
      name = get (myKeys[4]);
      lang = get (myKeys[5]);
    }
    @Override
    protected void putCached ()
    {
      super.putCached ();
      put (myKeys[0], Boolean.valueOf (verbose).toString ());
      put (myKeys[1], Boolean.valueOf (pedantic).toString ());
      if (user != null)
        put (myKeys[2], user);
      if (pass != null)
        put (myKeys[3], pass);
      put (myKeys[4], name);
      put (myKeys[5], lang);
    }
  }

  private final ServerInfo serverInfo = new ServerInfo ();
  private final ClientInfo clientInfo = new ClientInfo ();

  public ClientHandshakeHandler (EventLoopGroup workerGroup,
                                 EventLoopGroup eventExecutorGroup,
                                 URI uri,
                                 Options options,
                                 ClientConnection handler)
  {
    this.workerGroup = (workerGroup != null
                        ? workerGroup
                        : new NioEventLoopGroup ());
    this.eventExecutorGroup = (eventExecutorGroup != null
                               ? eventExecutorGroup
                               : new  DefaultEventExecutorGroup (1));
    this.uri = uri;
    this.options = options;
    this.state = States.START;
    this.handler = handler;
  }

  private ScheduledFuture watchDogFuture;
  private ChannelFuture connectFuture;
  private ChannelFuture writeFuture;

  private void transition (States newState)
  {
    if (DEBUG){
      System.err.printf ("%s: transition to %s from %s (done=%s watchDogFuture.isCancelled=%s)\n",
                         this,
                         newState,
                         state,
                         done (),
                         (watchDogFuture != null
                          && watchDogFuture.isCancelled ()));
      System.err.flush ();
    }
    final boolean wasDone = done ();
    state = newState;
    if (done () && wasDone == false)
      {
        final boolean success = state == States.HANDSHAKE_COMPLETE;
        if (success == false && channel != null)
          {
            // Do this before cancelling futures, our listeners
            Channel t = channel;
            channel = null;
            t.close ();
            if (DEBUG){
              System.err.printf ("%s: transition closing channel %s\n",
                                 this,
                                 t);
              System.err.flush();
            }
          }

        if (watchDogFuture != null && watchDogFuture.isCancelled () == false)
          {
            final boolean res = watchDogFuture.cancel (true);
            watchDogFuture = null;
            if (DEBUG){
            System.err.printf ("%s: transition cancelled watchDog: res=%s\n",
                               this,
                               res);
            System.err.flush();
           }
          }
        if (connectFuture != null)
          {
            final boolean res = connectFuture.cancel (true);
            connectFuture = null;
            if (DEBUG){
              System.err.printf ("%s: transition cancelled connect: res=%s\n",
                                 this,
                                 res);
              System.err.flush();
            }

          }
        if (writeFuture != null)
          {
            final boolean res = writeFuture.cancel (true);
            writeFuture = null;
            if (DEBUG){
              System.err.printf ("%s: transition cancelled write: res=%s\n",
                                 this,
                                 res);
              System.err.flush();
            }
          }
        if (sslHandshakeFuture != null)
          {
            final boolean res = sslHandshakeFuture.cancel (true);
            sslHandshakeFuture = null;
            if (DEBUG){
              System.err.printf ("%s: transition cancelled ssl handshake: res=%s\n",
                                 this,
                                 res);
              System.err.flush();
            }
          }
        channel.pipeline ().remove (this);
        if (handler != null)
          handler.handshakeCompleted (success, channel, serverInfo, error);
      }
  }

  public void exceptionCaught (ChannelHandlerContext ctx, Throwable cause)
  {
    if (DEBUG) {
      System.err.printf ("%s: caught exception: %s", this, cause);
      System.err.flush ();
    }
    error = cause;
    transition (States.CONNECT_ERR);
  }

  private final Runnable TIMER_EXPIRED = new Runnable () {
    public void run ()
    {
      if (DEBUG) {
      System.err.printf ("%s: timer run (fuse up=%s)\n",
                         ClientHandshakeHandler.this,
                         deadline <= System.currentTimeMillis ());
      System.err.flush ();
      }
      if (watchDogFuture.isCancelled () == false && ! done ())
        transition (States.CONNECT_TIMEOUT);
    }
  };

  public void connect ()
  {
    final String host = uri.getHost();
    final int uri_port = uri.getPort ();
    final int port = uri_port >= 0
                     ? uri_port : Options.DEFAULT_PORT;

    startTime = System.currentTimeMillis ();
    deadline = startTime + TimeUnit.SECONDS.toMillis (options.timeout);

    if (DEBUG){
    System.err.printf ("%s: connect starting\n", this);
    System.err.flush ();
    }
    watchDogFuture = eventExecutorGroup.schedule (TIMER_EXPIRED, options.timeout, TimeUnit.SECONDS);
    watchDogFuture.addListener (this);

    Bootstrap b = new Bootstrap ();
    b.group (workerGroup)
     .channel (NioSocketChannel.class)
        // FIXME: jam: set channel options here for client settings
        // FIXME: jam: specifically: high/low watermarks, tcp settings
        // FIXME: jam: and allocators (pooled, referenced counted) for buffers and messages
//	.option (ChannelOption.TCP_NODELAY, true)
     .handler (new ChannelInitializer<SocketChannel> () {
       @Override
       public void initChannel (SocketChannel chan) throws Exception {
         ChannelPipeline pipe = chan.pipeline ();
         if (options.tracePackets || DEBUG)
           pipe.addLast ("nats-packet-tracer", new LoggingHandler (LogLevel.INFO));
         pipe.addLast ("nats-packet-decoder", new NatsMessageDecoder (options.maxFrameSize, true));
         pipe.addLast ("nats-packet-encoder", new MessageEncoder ());
         if (options.traceProtocol || DEBUG)
           pipe.addLast ("nats-protocol-tracer", new LoggingHandler (LogLevel.INFO));
         pipe.addLast ("nats-client-handshaker", ClientHandshakeHandler.this);
         if (options.secure)
            {
//              String[] TLS_CIPHERS[] = {
//
//              };
              // FIXME: jam: move this to options and specify ciphers, etc. check with team
              // FIXME: jam: on supported combos??
              sslContext = (SslContextBuilder.forClient()
                                             .trustManager (InsecureTrustManagerFactory.INSTANCE)
                                             .clientAuth (ClientAuth.NONE)
//                                             .ciphers (ArrayList<String> (Arrays.asList (TLS_CIPHERS)))
                                             .build ());
              sslHandler = sslContext.newHandler (chan.alloc (), host, port);
              if (DEBUG) {
                System.out.flush ();
                System.out.printf ("%s: sslContext=%s sslHandler=%s\n", this, sslContext, sslHandler);
              }
            }
       }
     });
    transition (States.CONNECTING);
    if (DEBUG){
    System.err.printf ("%s: connecting to %s (timeout=%s)\n", this, uri,
                       TimeUnit.MILLISECONDS.toSeconds (deadline - startTime));
    System.err.flush ();
    }
    (connectFuture = b.connect (host, port)).addListener (this);
  }

  public String toString ()
  {
    return String.format ("handshake(%s) to %s start=%s deadline=%s left=%s state=%s",
                          super.toString (),
                          uri,
                          startTime,
                          deadline,
                          deadline - System.currentTimeMillis (),
                          state);
  }
  public void operationComplete (Future future)
    throws Exception
  {
    if (DEBUG){
    System.err.printf ("@@@ %s: future operationComplete (%s)\n", this, future);
    System.err.flush ();
    }
    /*
    if (state != States.CONNECTING && state != States.AWAITING_ACK)
      return;
      */
    if (future.isCancelled ())
      {
        // someone cancelled our connect, write or ssl handshake
        assert (future == connectFuture ||  future == writeFuture || future == sslHandshakeFuture);
        if (DEBUG){
        System.err.printf ("%s: future cancelled %s\n", this, future);
        System.err.flush ();
        }
        transition (States.CONNECT_CANCELLED);
      }
    else if (! future.isSuccess ())
      {
        Throwable cause = future.cause ();

        // our connect, write or ssl handshake failed :-(
        assert (future == connectFuture || future == writeFuture || future == sslHandshakeFuture);
        transition (States.CONNECT_FAILURE);
        failureCause = cause;
        if (cause instanceof java.net.ConnectException
            && cause.getMessage ().toLowerCase ().contains ("connection refused"))
          failureCause = null;
        if (DEBUG){
        System.err.printf ("%s: future failed %s->%s (%s)\n",
                           this, cause, failureCause, future);
        cause.printStackTrace (System.err);
        System.err.flush ();
        }
      }
    // for the remaining arms we are done with the future, its completed
    // before our watchdog timeout...
    else if (state == States.CONNECTING)
      {
        assert (future == connectFuture);
        assert (channel == null);
        channel = ((ChannelFuture) future).channel ();
        transition (States.CONNECTED);
        if(DEBUG){
        System.err.printf ("%s: future connected to %s (%s)\n", this, channel, future);
        System.err.flush ();
        }
      }
    else if (state == States.AWAITING_SSL_HANDSHAKE)
      {
        // our SSL handshake is done, write the CONNECT message
        assert (channel != null);
        assert (future == writeFuture);
        transition (States.AWAITING_ACK);
        (writeFuture = channel.writeAndFlush (connectMessage)).addListener (this);
        if (DEBUG) {
          System.err.printf ("%s: ssl handshake completed\n", this);
          System.err.flush ();
        }
      }
    else if (state == States.AWAITING_ACK)
      {
        assert (channel != null);
        assert (future == writeFuture);
        // our INFO/CONNECT succeeded...
        // replace all the pipeline with whatever the user wants...
        transition (States.HANDSHAKE_COMPLETE);
        if (DEBUG) {
          System.err.println ("HANDSHAKE DONE @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
          System.err.flush ();
        }
      }
    else
      {
        throw new IllegalStateException (String.format ("%s: unexpected future for state %s: %s",
                                                        state, future));
      }
  }

  public void infoReceived (ChannelHandlerContext ctx, java.util.Map<String,String> params)
    throws Exception
  {
    serverInfo.clear ();
    serverInfo.putAll (params);
    serverInfo.updateCached ();

    clientInfo.clear ();
    clientInfo.updateCached ();

    if (options.secure && !serverInfo.tls_required)
      throw new NatsException (NatsException.ErrSecureConnWanted);
    if (serverInfo.tls_required && !options.secure)
      throw new NatsException (NatsException.ErrSecureConnRequired);

    String userInfo = uri.getUserInfo ();
    if (userInfo != null)
    {
      final int split = userInfo.indexOf (':');
      if (split != -1 && split < userInfo.length () - 1)
      {
        clientInfo.user = userInfo.substring (0, split);
        clientInfo.pass = userInfo.substring (split + 1);

      }
    }
    clientInfo.verbose = options.verbose;
    clientInfo.pedantic = options.pedantic;
    clientInfo.tls_required = options.secure;
    clientInfo.name = options.name;
    clientInfo.lang = LANGUAGE;
    clientInfo.version = VERSION;
    clientInfo.putCached ();
    connectMessage = Message.CONNECT (clientInfo);

    if (options.secure)
      {
        assert (sslHandler != null);
        transition (States.AWAITING_SSL_HANDSHAKE);
        (sslHandshakeFuture = sslHandler.handshakeFuture ()).addListener (this);
        //channel.pipeline ().addLast (sslHandler);
        channel.pipeline ().addFirst (sslHandler);
        if (DEBUG){
          System.err.printf ("%s: starting ssl handshake... (%s)\n", this, sslHandshakeFuture);
        }
      }
    else
      {
        transition (States.AWAITING_ACK);
        (writeFuture = ctx.writeAndFlush (connectMessage)).addListener (this);
      }
  }

  @Override
  public void channelRead (ChannelHandlerContext ctx, Object msg)
    throws Exception
  {
    if (DEBUG){
    System.err.printf ("%s: handshake channelRead: %s\n", this, msg);
    System.err.flush ();
    }
    if (state != States.CONNECTED
        && msg instanceof Message.INFO == false)
      {
        if (true || DEBUG){
          System.err.printf ("%s: unexpected message for this state\n", this);
          System.err.flush ();
        }
      }
    else
      {
        Message.INFO inf = (Message.INFO) msg;
        infoReceived (ctx, inf.handshake_params);
      }
  }
}
