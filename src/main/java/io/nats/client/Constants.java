/**
 * 
 */
package io.nats.client;

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
}
