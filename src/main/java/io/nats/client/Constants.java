/**
 * 
 */
package io.nats.client;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public final class Constants {

	private Constants() {}
	
	// Default server host
	public static final String	DEFAULT_HOST		= "localhost";
	// Default server port
	public static final int DEFAULT_PORT			= 4222;
	// Default server URL
	public static final String 	DEFAULT_URL				= 
			"nats://" + DEFAULT_HOST+ ":" + DEFAULT_PORT;	
	// Default SSL/TLS protocol version
	static final String		DEFAULT_SSL_PROTOCOL 	= "TLSv1.2";
	// Default maximum number of reconnect attempts.
	static final int		DEFAULT_MAX_RECONNECT	= 60;
	// Default wait time before attempting reconnection to the same server
	static final int		DEFAULT_RECONNECT_WAIT	= 2 * 1000;
	// Default connection timeout
	static final int		DEFAULT_TIMEOUT			= 2 * 1000;
	// Default ping interval; <=0 means disabled
	static final int 		DEFAULT_PING_INTERVAL	= 2 * 60000;
	// Default maximum number of pings that have not received a response
	static final int		DEFAULT_MAX_PINGS_OUT	= 2;
	// Default maximum channel length
	static final int		DEFAULT_MAX_CHAN_LEN	= 65536;
	//  Maximum size of a control line (message header)
	final static int 		MAX_CONTROL_LINE_SIZE = 1024;
	
	public static enum ConnState {
		DISCONNECTED, CONNECTED, CLOSED, RECONNECTING, CONNECTING
	}

	// Property names
	final static String PFX = "io.nats.client.";
	final static String PROP_URL 					= PFX + "url";
	final static String PROP_HOST 					= PFX + "host";
	final static String PROP_PORT 					= PFX + "port";
	final static String PROP_USERNAME 				= PFX + "username";
	final static String PROP_PASSWORD 				= PFX + "password";
	final static String PROP_SERVERS 				= PFX + "servers";
	final static String PROP_NORANDOMIZE			= PFX + "norandomize";
	final static String PROP_CONNECTION_NAME 		= PFX + "name";
	final static String PROP_VERBOSE 				= PFX + "verbose";
	final static String PROP_PEDANTIC 				= PFX + "pedantic";
	final static String PROP_SECURE 				= PFX + "secure";
	final static String PROP_RECONNECT_ALLOWED		= PFX + "reconnect.allowed";
	final static String PROP_MAX_RECONNECT			= PFX + "reconnect.max";
	final static String PROP_RECONNECT_WAIT 		= PFX + "reconnect.wait";
	final static String PROP_CONNECTION_TIMEOUT		= PFX + "timeout";
	final static String PROP_PING_INTERVAL 			= PFX + "pinginterval";
	final static String PROP_MAX_PINGS 				= PFX + "maxpings";
	final static String PROP_EXCEPTION_HANDLER		= PFX + "handler.exception";
	final static String PROP_CLOSED_HANDLER			= PFX + "handler.closed";
	final static String PROP_DISCONNECTED_HANDLER	= PFX + "handler.disconnected";
	final static String PROP_RECONNECTED_HANDLER	= PFX + "handler.reconnected";
}
