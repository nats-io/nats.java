/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats;

import java.net.URI;

public interface Context
{
  /**
   * Create a new connection given the options specified in <i>opts</i>
   * @param opts the options used to configure the given logical connection
   * @return a new logical Connection
   * @throws NatsException if the connection is not able to be created or
   * there are any invalid options.
   */
  public Connection connect (Options opts)
    throws NatsException;

  /**
   * Create a new connection given the URI string specified in <i>uri</i> and default options
   * for this context.
   * @param uri the NATS uri used for the logical connection
   * @return a new logical connection
   * @throws NatsException if the connection is not able to be created
   * @throws java.net.URISyntaxException if the specified <i>URI</i> is invalid
   * @see #connect(Options)
   */
  public Connection connect (String uri)
    throws NatsException, java.net.URISyntaxException;

  /**
   * Create a new connection with a default set of options for this context.
   * @return a new logical connection
   * @throws NatsException if the connection is not able to be created
   * @see Options
   */
  public Connection connect ()
    throws NatsException;

  /**
   * Create a new connection given the URI using the default options for this context.
   * @param uri the NATS uri used for the logical connection
   * @return a new logical connection
   * @throws NatsException if the connection is not able to be created
   * @see #connect(Options)
   */
  public Connection connect (URI uri)
    throws NatsException;


  /**
   * Create a new  secure connection given the URI using the default options for this context.
   * @return a new logical connection
   * @throws NatsException if the connection is not able to be created
   * @see #connect(Options)
   */
  public Connection secureConnect ()
  throws NatsException;

  /**
   * Create a new  secure connection given the URI using the default options for this context.
   * @return a new logical connection
   * @throws NatsException if the connection is not able to be created
   * @see #connect(Options)
   */
  public Connection secureConnect (URI uri)
  throws NatsException;
}
