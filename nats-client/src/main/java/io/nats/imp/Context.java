/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats.imp;

import io.nats.NatsException;
import io.nats.Options;
import io.netty.util.internal.ConcurrentSet;

import java.net.URI;

/**
 * Interface to manage a group of logical
 * NATS Connections. Each logical connection may have several
 * backing connections.
 *
 * @see Options
 * @see io.nats.Connection
 */
public class Context
  implements AutoCloseable, io.nats.Context
{
  private final Options defaults;
  private final ConcurrentSet<io.nats.Connection> connections;

  public Context (Options defaults)
  throws NatsException
  {
    this.defaults = new Options (defaults);
    this.connections = new ConcurrentSet<io.nats.Connection> ();

    if (this.defaults.maxPingsOut <= 0)
      this.defaults.maxPingsOut = Options.DEFAULT_MAX_PINGS_OUT;
    if (this.defaults.subChanLen <= 0)
      this.defaults.subChanLen = Options.DEFAULT_ASYNC_MAX_CHAN_LEN;
  }

  public Context ()
    throws NatsException
  {
    this (new Options ());
  }

  /**
   * Create a new connection given the options specified in <i>opts</i>
   * @param opts the options used to configure the given logical connection
   * @return a new logical Connection
   * @throws NatsException if the connection is not able to be created or
   * there are any invalid options.
   */
  public io.nats.Connection connect (Options opts)
    throws NatsException
  {
    ClientConnection res;
    res = new ClientConnection (this, opts != null ? opts : defaults);
    res._setupAndRun ();
    return res;
  }

  /**
   * Create a new connection given the URI string specified in <i>uri</i> and default options
   * for this context.
   * @param uri the NATS uri used for the logical connection
   * @return a new logical connection
   * @throws NatsException if the connection is not able to be created
   * @throws java.net.URISyntaxException if the specified <i>URI</i> is invalid
   * @see #connect(Options)
   */
  public io.nats.Connection connect (String uri)
    throws NatsException, java.net.URISyntaxException
  {
    return connect (new URI (uri));
  }

  /**
   * Create a new connection with a default set of options for this context.
   * @return a new logical connection
   * @throws NatsException if the connection is not able to be created
   * @see Options
   */
  public io.nats.Connection connect ()
    throws NatsException
  {
    return connect ((Options) null);
  }

  /**
   * Create a new connection given the URI using the default options for this context.
   * @param uri the NATS uri used for the logical connection
   * @return a new logical connection
   * @throws NatsException if the connection is not able to be created
   * @see #connect(Options)
   */
  public io.nats.Connection connect (URI uri)
    throws NatsException
  {
    Options opts = new Options ();
    opts.servers.add (uri);
    return connect (opts);
  }

  /**
   * Create a new  secure connection given the URI using the default options for this context.
   * @return a new logical connection
   * @throws NatsException if the connection is not able to be created
   * @see #connect(Options)
   */
  public io.nats.Connection secureConnect ()
   throws NatsException
  {
    Options o = new Options ();
    o.secure = true;
    return connect (o);
  }
  /**
   * Create a new  secure connection given the URI using the default options for this context.
   * @return a new logical connection
   * @throws NatsException if the connection is not able to be created
   * @see #connect(Options)
   */
  public io.nats.Connection secureConnect (URI uri)
   throws NatsException
  {
    Options o = new Options ();
    o.secure = true;
    return connect (o);
  }
  public void close ()
    throws NatsException
  {
    for (io.nats.Connection c : connections)
      c.close ();
  }
}