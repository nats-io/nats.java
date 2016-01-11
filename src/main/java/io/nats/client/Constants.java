/*******************************************************************************
 * Copyright (c) 2012, 2015 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
/**
 * 
 */
package io.nats.client;

/**
 * Constants for io.nats.client
 *
 */
public final class Constants {

	private Constants() {}
	
	/**
	 * 	Default server host
	 * <p>
	 * This property is defined as String {@value #DEFAULT_HOST}
	 */
	public static final String	DEFAULT_HOST		= "localhost";
	/**
	 * Default server port
	 * <p>
	 * This property is defined as int {@value #DEFAULT_PORT}
	 */
	public static final int DEFAULT_PORT			= 4222;
	/**
	 * Default server URL 
	 * <p>
	 * This property is defined as String {@value #DEFAULT_URL}
	 */
	public static final String 	DEFAULT_URL				= 
			"nats://" + DEFAULT_HOST+ ":" + DEFAULT_PORT;	
	/**
	 * Default SSL/TLS protocol version
	 * <p>
	 * This property is defined as String {@value #DEFAULT_SSL_PROTOCOL}
	 */
	static final String		DEFAULT_SSL_PROTOCOL 	= "TLSv1.2";
	/**
	 * Default maximum number of reconnect attempts. 
	 * <p>
	 * This property is defined as String {@value #DEFAULT_MAX_RECONNECT}
	 */
	static final int		DEFAULT_MAX_RECONNECT	= 60;
	/**
	 * Default wait time before attempting reconnection to the same server 
	 * <p>
	 * This property is defined as String {@value #DEFAULT_RECONNECT_WAIT}
	 */
	static final int		DEFAULT_RECONNECT_WAIT	= 2 * 1000;
	/**
	 * Default connection timeout
	 * <p>
	 * This property is defined as String {@value #DEFAULT_TIMEOUT}
	 */
	static final int		DEFAULT_TIMEOUT			= 2 * 1000;
	/**
	 * Default ping interval; <=0 means disabled
	 * <p>
	 * This property is defined as String {@value #DEFAULT_PING_INTERVAL}
	 */
	static final int 		DEFAULT_PING_INTERVAL	= 2 * 60000;
	/**
	 * Default maximum number of pings that have not received a response
	 * <p>
	 * This property is defined as String {@value #DEFAULT_MAX_PINGS_OUT}
	 */
	static final int		DEFAULT_MAX_PINGS_OUT	= 2;
	/**
	 * Default maximum channel length
	 * <p>
	 * This property is defined as String {@value #DEFAULT_MAX_CHAN_LEN}
	 */
	static final int		DEFAULT_MAX_CHAN_LEN	= 65536;

	/**
	 * Maximum size of a control line (message header)
	 */
	final static int 		MAX_CONTROL_LINE_SIZE = 1024;
	
	/**
	 * Connection states for {@link Connection#getState()}
	 */
	public static enum ConnState {
		/**
		 * The {@code Connection} is currently disconnected
		 */
		DISCONNECTED, 
		/**
		 * The {@code Connection} is currently connected
		 */
		CONNECTED, 
		/**
		 * The {@code Connection} is currently closed
		 */
		CLOSED, 
		/**
		 * The {@code Connection} is currently attempting to reconnect
		 * to a server it was previously connected to
		 * @see Connection#isReconnecting()
		 */
		RECONNECTING, 
		/**
		 * The {@code Connection} is currently connecting to a server
		 * for the first time
		 */
		CONNECTING
	}

	// Property names
	/**
	 * 
	 */
	final static String PFX = "io.nats.client.";
	/**
	 * This property is defined as String {@value #PROP_URL}
	 */
	public final static String PROP_URL 					= PFX + "url";
	/**
	 * This property is defined as String {@value #PROP_HOST}
	 */
	public final static String PROP_HOST 					= PFX + "host";
	/**
	 * This property is defined as String {@value #PROP_PORT}
	 */
	public final static String PROP_PORT 					= PFX + "port";
	/**
	 * This property is defined as String {@value #PROP_USERNAME}
	 */
	public final static String PROP_USERNAME 				= PFX + "username";
	/**
	 * This property is defined as String {@value #PROP_PASSWORD}
	 */
	public final static String PROP_PASSWORD 				= PFX + "password";
	/**
	 * This property is defined as String {@value #PROP_SERVERS}
	 */
	public final static String PROP_SERVERS 				= PFX + "servers";
	/**
	 * This property is defined as String {@value #PROP_NORANDOMIZE}
	 */
	public final static String PROP_NORANDOMIZE			= PFX + "norandomize";
	/**
	 * This property is defined as String {@value #PROP_CONNECTION_NAME}
	 */
	public final static String PROP_CONNECTION_NAME 		= PFX + "name";
	/**
	 * This property is defined as String {@value #PROP_VERBOSE}
	 */
	public final static String PROP_VERBOSE 				= PFX + "verbose";
	/**
	 * This property is defined as String {@value #PROP_PEDANTIC}
	 */
	public final static String PROP_PEDANTIC 				= PFX + "pedantic";
	/**
	 * This property is defined as String {@value #PROP_SECURE}
	 */
	public final static String PROP_SECURE 				= PFX + "secure";
	/**
	 * This property is defined as String {@value #PROP_TLS_DEBUG}
	 */
	public final static String PROP_TLS_DEBUG 			= PFX + "tls.debug";
	/**
	 * This property is defined as String {@value #PROP_RECONNECT_ALLOWED}
	 */
	public final static String PROP_RECONNECT_ALLOWED		= PFX + "reconnect.allowed";
	/**
	 * This property is defined as String {@value #PROP_MAX_RECONNECT}
	 */
	public final static String PROP_MAX_RECONNECT			= PFX + "reconnect.max";
	/**
	 * This property is defined as String {@value #PROP_RECONNECT_WAIT}
	 */
	public final static String PROP_RECONNECT_WAIT 		= PFX + "reconnect.wait";
	/**
	 * This property is defined as String {@value #PROP_CONNECTION_TIMEOUT}
	 */
	public final static String PROP_CONNECTION_TIMEOUT		= PFX + "timeout";
	/**
	 * This property is defined as String {@value #PROP_PING_INTERVAL}
	 */
	public final static String PROP_PING_INTERVAL 			= PFX + "pinginterval";
	/**
	 * This property is defined as String {@value #PROP_MAX_PINGS}
	 */
	public final static String PROP_MAX_PINGS 				= PFX + "maxpings";
	/**
	 * This property is defined as String {@value #PROP_EXCEPTION_HANDLER}
	 */
	public final static String PROP_EXCEPTION_HANDLER		= PFX + "callback.exception";
	/**
	 * This property is defined as String {@value #PROP_CLOSED_CB}
	 */
	public final static String PROP_CLOSED_CB				= PFX + "callback.closed";
	/**
	 * This property is defined as String {@value #PROP_DISCONNECTED_CB}
	 */
	public final static String PROP_DISCONNECTED_CB		= PFX + "callback.disconnected";
	/**
	 * This property is defined as String {@value #PROP_RECONNECTED_CB}
	 */
	public final static String PROP_RECONNECTED_CB			= PFX + "callback.reconnected";
}
