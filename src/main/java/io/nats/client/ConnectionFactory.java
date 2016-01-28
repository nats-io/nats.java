/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.SSLContext;

import static io.nats.client.Constants.*;
/* *
 * Factory class for opening a {@link ConnectionImpl} to the NATS server (gnatsd). 
 */

/**
 * A {@code ConnectionFactory} object encapsulates a set of connection configuration
 * options. A client uses it to create a connection to NATS.

 */
public class ConnectionFactory implements Cloneable {

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
	public static final int		DEFAULT_MAX_RECONNECT	= 60;
	/**
	 * Default wait time before attempting reconnection to the same server 
	 * <p>
	 * This property is defined as String {@value #DEFAULT_RECONNECT_WAIT}
	 */
	public static final int		DEFAULT_RECONNECT_WAIT	= 2 * 1000;
	/**
	 * Default connection timeout
	 * <p>
	 * This property is defined as String {@value #DEFAULT_TIMEOUT}
	 */
	public static final int		DEFAULT_TIMEOUT			= 2 * 1000;
	/**
	 * Default server ping interval. {@code <=0} means disabled
	 * <p>
	 * This property is defined as String {@value #DEFAULT_PING_INTERVAL}
	 */
	public static final int 		DEFAULT_PING_INTERVAL	= 2 * 60000;
	/**
	 * Default maximum number of pings that have not received a response
	 * <p>
	 * This property is defined as String {@value #DEFAULT_MAX_PINGS_OUT}
	 */
	public static final int		DEFAULT_MAX_PINGS_OUT	= 2;
	/**
	 * Default maximum channel length
	 * <p>
	 * This property is defined as String {@value #DEFAULT_MAX_PENDING_MSGS}
	 */
	public static final int		DEFAULT_MAX_PENDING_MSGS	= 65536;


	
	private URI url									= null;
	private String host								= null;
	private int port								= -1;
	private String username							= null;
	private String password							= null;
	private List<URI> servers						= new ArrayList<URI>();	
	private boolean noRandomize						= false;
	private String connectionName					= null;
	private boolean verbose							= false;
	private boolean pedantic						= false;
	private boolean secure							= false;
	private boolean reconnectAllowed				= true;
	private int maxReconnect						= DEFAULT_MAX_RECONNECT;
	private long reconnectWait						= DEFAULT_RECONNECT_WAIT;
	private int connectionTimeout					= DEFAULT_TIMEOUT;
	private long pingInterval						= DEFAULT_PING_INTERVAL;
	private int maxPingsOut							= DEFAULT_MAX_PINGS_OUT;
	private SSLContext sslContext;
	private ExceptionHandler exceptionHandler;
	private ClosedCallback closedCallback;
	private DisconnectedCallback disconnectedCallback;
	private ReconnectedCallback reconnectedCallback;

	private String urlString 						= null;

	// The size of the buffered channel used for message delivery or sync 
	// subscription.
	private int maxPendingMsgs						= DEFAULT_MAX_PENDING_MSGS;
	private boolean tlsDebug;

	/**
	 * Constructs a new connection factory from a {@link Properties} object
	 * @param props the {@link Properties} object
	 */
	public ConnectionFactory(Properties props) {
		if (props==null)
			throw new IllegalArgumentException("Properties cannot be null");

		//PROP_URL
		if (props.containsKey(PROP_URL))
			this.setUrl(props.getProperty(PROP_URL, DEFAULT_URL));
		//PROP_HOST
		if (props.containsKey(PROP_HOST))
			this.setHost(props.getProperty(PROP_HOST, DEFAULT_HOST));
		//PROP_PORT
		if (props.containsKey(PROP_PORT)) {
			this.setPort(Integer.parseInt(props.getProperty(PROP_PORT, 
					Integer.toString(DEFAULT_PORT))));
		}
		//PROP_USERNAME
		if (props.containsKey(PROP_USERNAME))
			this.setUsername(props.getProperty(PROP_USERNAME, null));
		//PROP_PASSWORD
		if (props.containsKey(PROP_PASSWORD))
			this.setPassword(props.getProperty(PROP_PASSWORD, null));
		//PROP_SERVERS
		if (props.containsKey(PROP_SERVERS)) {
			String s = props.getProperty(PROP_SERVERS);
			if (s.isEmpty())
				throw new IllegalArgumentException(PROP_SERVERS + " cannot be empty");
			else {
				String[] servers = s.trim().split(",\\s*");
				this.setServers(servers);
			}
		}
		//PROP_NORANDOMIZE
		if (props.containsKey(PROP_NORANDOMIZE))
			this.setNoRandomize(Boolean.parseBoolean(props.getProperty(PROP_NORANDOMIZE)));
		//PROP_CONNECTION_NAME
		if (props.containsKey(PROP_CONNECTION_NAME))
			this.setConnectionName(props.getProperty(PROP_CONNECTION_NAME, null));
		//PROP_VERBOSE
		if (props.containsKey(PROP_VERBOSE))
			this.setVerbose(Boolean.parseBoolean(props.getProperty(PROP_VERBOSE)));
		//PROP_PEDANTIC
		if (props.containsKey(PROP_PEDANTIC))
			this.setPedantic(Boolean.parseBoolean(props.getProperty(PROP_PEDANTIC)));
		//PROP_SECURE
		if (props.containsKey(PROP_SECURE))
			this.setSecure(Boolean.parseBoolean(props.getProperty(PROP_SECURE)));
		//PROP_TLS_DEBUG
		if (props.containsKey(PROP_TLS_DEBUG))
			this.setTlsDebug(Boolean.parseBoolean(props.getProperty(PROP_TLS_DEBUG)));
		//PROP_RECONNECT_ALLOWED
		if (props.containsKey(PROP_RECONNECT_ALLOWED))
			this.setReconnectAllowed(Boolean.parseBoolean(
					props.getProperty(PROP_RECONNECT_ALLOWED, Boolean.toString(true))));
		//PROP_MAX_RECONNECT
		if (props.containsKey(PROP_MAX_RECONNECT))
			this.setMaxReconnect(Integer.parseInt(
					props.getProperty(PROP_MAX_RECONNECT, Integer.toString(DEFAULT_MAX_RECONNECT))));
		//PROP_RECONNECT_WAIT
		if (props.containsKey(PROP_RECONNECT_WAIT))
			this.setReconnectWait(Integer.parseInt(
					props.getProperty(PROP_RECONNECT_WAIT, Integer.toString(DEFAULT_RECONNECT_WAIT))));
		//PROP_CONNECTION_TIMEOUT
		if (props.containsKey(PROP_CONNECTION_TIMEOUT))
			this.setConnectionTimeout(Integer.parseInt(
					props.getProperty(PROP_CONNECTION_TIMEOUT, Integer.toString(DEFAULT_TIMEOUT))));
		//PROP_PING_INTERVAL
		if (props.containsKey(PROP_PING_INTERVAL))
			this.setPingInterval(Integer.parseInt(
					props.getProperty(PROP_PING_INTERVAL, Integer.toString(DEFAULT_PING_INTERVAL))));
		//PROP_MAX_PINGS
		if (props.containsKey(PROP_MAX_PINGS))
			this.setMaxPingsOut(Integer.parseInt(
					props.getProperty(PROP_MAX_PINGS, Integer.toString(DEFAULT_MAX_PINGS_OUT))));
		//PROP_EXCEPTION_HANDLER
		if (props.containsKey(PROP_EXCEPTION_HANDLER)) {
			Object instance = null;
			try {
				String s = props.getProperty(PROP_EXCEPTION_HANDLER);
				Class<?> clazz = Class.forName(s);
				Constructor<?> constructor = clazz.getConstructor();
				instance = constructor.newInstance();
			} catch (Exception e) {
				throw new IllegalArgumentException(e);
			} finally {}
			this.setExceptionHandler((ExceptionHandler) instance);
		}
		//PROP_CLOSED_CB
		if (props.containsKey(PROP_CLOSED_CB)) {
			Object instance = null;
			try {
				String s = props.getProperty(PROP_CLOSED_CB);
				Class<?> clazz = Class.forName(s);
				Constructor<?> constructor = clazz.getConstructor();
				instance = constructor.newInstance();
			} catch (Exception e) {
				throw new IllegalArgumentException(e);
			} finally {}
			this.setClosedCallback((ClosedCallback) instance);
		}
		//PROP_DISCONNECTED_CB
		if (props.containsKey(PROP_DISCONNECTED_CB)) {
			Object instance = null;
			try {
				String s = props.getProperty(PROP_DISCONNECTED_CB);
				Class<?> clazz = Class.forName(s);
				Constructor<?> constructor = clazz.getConstructor();
				instance = constructor.newInstance();
			} catch (Exception e) {
				throw new IllegalArgumentException(e);
			} finally {}
			this.setDisconnectedCallback((DisconnectedCallback) instance);
		}
		//PROP_RECONNECTED_CB
		if (props.containsKey(PROP_RECONNECTED_CB)) {
			Object instance = null;
			try {
				String s = props.getProperty(PROP_RECONNECTED_CB);
				Class<?> clazz = Class.forName(s);
				Constructor<?> constructor = clazz.getConstructor();
				instance = constructor.newInstance();
			} catch (Exception e) {
				throw new IllegalArgumentException(e);
			} finally {}
			this.setReconnectedCallback((ReconnectedCallback) instance);
		}
		//PROP_MAX_PENDING_MSGS
		if (props.containsKey(PROP_MAX_PENDING_MSGS))
			this.setMaxPendingMsgs(Integer.parseInt(
					props.getProperty(PROP_MAX_PENDING_MSGS, Integer.toString(DEFAULT_MAX_PENDING_MSGS))));

	}

	/**
	 * 
	 */
	public ConnectionFactory() {
		this(null, null);
	}

	/**
	 * Constructs a connection factory using the supplied URL string
	 * as default
	 * @param url the default server URL to use
	 */
	public ConnectionFactory(String url)
	{
		this(url, null);
	}

	/**
	 * Constructs a connection factory from a list of NATS server
	 * URL strings
	 * @param servers the list of cluster server URL strings
	 */
	public ConnectionFactory(String[] servers) {
		this(null, servers);
	}
	
	/**
	 * Constructs a connection factory from a list of NATS server
	 * URLs, using {@code url} as the primary address.
	 * <p>
	 * If {@code url} contains a single server address, that 
	 * address will be first in the server list, even if 
	 * {@link #isNoRandomize()} is {@code false}.
	 * <p>
	 * If {@code url} is a comma-delimited list of servers,
	 * then {@code servers} will be ignored.
	 * @param url the default server URL to set
	 * @param servers the list of cluster server URL strings
	 */
	public ConnectionFactory(String url, String[] servers)
	{
		if (url != null && url.contains(",")) {
			this.setServers(url);
		} else {
			this.setUrl(url);
			this.setServers(servers);
		}
	}

	/**
	 * Creates an active connection to a NATS server
	 * @return the Connection.
	 * @throws IOException if a Connection cannot be established for some reason.
	 * @throws TimeoutException if the connection timeout has been exceeded.
	 */
	public ConnectionImpl createConnection() throws IOException, TimeoutException {
		return createConnection(null);
	}

	// For unit test/mock purposes only.
	ConnectionImpl createConnection(TCPConnection tcpconn) throws IOException, TimeoutException {
		ConnectionImpl conn = null;
		Options options = options();

		conn = new ConnectionImpl(options, tcpconn);

		conn.connect();

		return conn;
	}

	protected URI constructURI() {
		URI res = null;
		if (url != null) {
			res = url;
		} else {
			String str = null;
			if (getHost() != null) {
				str = "nats://";
				if (getUsername() != null) { 
					str = str.concat(getUsername());
					if (getPassword() != null) {
						str = str.concat(":" + getPassword());
					}
					str = str.concat("@");
				}
				str = str.concat(getHost() + ":");
				if (getPort() > -1) {
					str = str.concat(String.valueOf(getPort()));
				} else {
					str = str.concat(String.valueOf(DEFAULT_PORT));
				}
				res = URI.create(str);
			}
		}
		return res;
	}
	protected Options options() {
		Options result = new Options();
		url = constructURI();
		result.setUrl(url);
		result.setHost(host);
		result.setPort(port);
		result.setPassword(password);
		result.setServers(servers);	
		result.setNoRandomize(noRandomize);
		result.setConnectionName(connectionName);
		result.setVerbose(verbose);
		result.setPedantic(pedantic);
		result.setSecure(secure);
		result.setTlsDebug(tlsDebug);
		result.setReconnectAllowed(reconnectAllowed);
		result.setMaxReconnect(maxReconnect);
		result.setReconnectWait(reconnectWait);
		result.setConnectionTimeout(connectionTimeout);
		result.setPingInterval(pingInterval);
		result.setMaxPingsOut(maxPingsOut);
		result.setExceptionHandler(exceptionHandler);
		result.setClosedCallback(closedCallback);
		result.setDisconnectedCallback(disconnectedCallback);
		result.setReconnectedCallback(reconnectedCallback);
		result.setMaxPendingMsgs(maxPendingMsgs);
		result.setSslContext(sslContext);
		return result;
	}

	/**
	 * Gets the maximum number of pending messages for a subscription. If
	 * this maximum is exceeded, an exception is thrown and the subscription
	 * is removed (unsubscribed by the connection).
	 * 
	 * @return the maximum number of pending messages allowable for this subscription
	 * @see #DEFAULT_MAX_PENDING_MSGS
	 */
	public int getMaxPendingMsgs() {
		return this.maxPendingMsgs;
	}

	/**
	 * Sets the maximum number of pending messages for a subscription. If
	 * this maximum is exceeded, an exception is thrown and the subscription
	 * is removed (unsubscribed by the connection).
	 * 
	 * @param max The maximum number of pending messages for a subscription
	 * @see #DEFAULT_MAX_PENDING_MSGS
	 */	
	public void setMaxPendingMsgs(int max) {
		this.maxPendingMsgs = max;
	}

	/** 
	 * {@inheritDoc}
	 */
	@Override public ConnectionFactory clone(){
		try {
			return (ConnectionFactory)super.clone();
		} catch (CloneNotSupportedException e) {
			throw new Error(e);
		}
	}

	/**
	 * Convenience function to set host, port, username, password from 
	 * a java.net.URI. Any omitted URI elements are left unchanged in the
	 * corresponding fields.
	 * @param uri the URI to set
	 */
	public void setUri(URI uri) {
		this.url = uri;

		String scheme = uri.getScheme().toLowerCase();
		if ("nats".equals(scheme) || "tcp".equals(scheme)) {
			// happy path
		} else {
			throw new IllegalArgumentException("Wrong scheme in NATS URI: " +
					uri.getScheme());
		}
		String host = uri.getHost();
		if (host != null) {
			setHost(host);
		}

		int port = uri.getPort();
		if (port != -1) {
			setPort(port);
		}

		String userInfo = uri.getUserInfo();
		if (userInfo != null) {
			String userpass[] = userInfo.split(":");
			if (userpass[0].length() > 0) {
				setUsername(userpass[0]);
				switch (userpass.length)
				{
				case 1:
					break;
				case 2:
					setPassword(userpass[1]);
					break;
				default:
					throw new IllegalArgumentException("Bad user info in NATS " +
							"URI: " + userInfo);
				}
			}
		}
	}

	/**
	 * Returns the default server URL string, if set
	 * @return the default server URL, or {@code null} if not set
	 */
	public String getUrlString() {
		return this.urlString;
	}

	/**
	 * Sets the default server URL string. 
	 * <p>
	 * If {@code url} is a comma-delimited
	 * list, then {@link #setServers(String)} will be invoked without 
	 * setting the default server URL string.
	 * @param url the URL to set
	 */
	public void setUrl(String url) {
		if (url==null) {
			this.url = null;
		} else if (url.contains(",")) {
			setServers(url);
		}
		else {
			try {
				this.setUri(new URI(url));
			} catch (NullPointerException | URISyntaxException e) {
				throw new IllegalArgumentException(e);
			}
			this.urlString=this.url.toString();
		}
	}

	/**
	 * Gets the default server host, if set
	 * @return the host, or {@code null} if not set
	 */
	public String getHost() {
		return this.host;
	}

	/**
	 * Sets the default server host
	 * @param host the host to set
	 */
	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * Gets the default server port, if set
	 * @return the default server port, or {@code -1} if not set
	 */
	public int getPort() {
		return this.port;
	}

	/**
	 * Sets the default server port
	 * @param port the port to set
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * Gets the default username, if set
	 * @return the username, or {@code null} if not set
	 */
	public String getUsername() {
		return this.username;
	}

	/**
	 * Sets the default username
	 * @param username the username to set
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * Gets the default password, or {@code null} if not set
	 * @return the password
	 */
	public String getPassword() {
		return this.password;
	}

	/**
	 * Sets the default password
	 * @param password the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * Gets the server list as {@code URI}
	 * @return the list of server {@code URI}s, or {@code null} if not set
	 */
	public List<URI> getServers() {
		return this.servers;
	}

	/**
	 * Sets the server list from a list of {@code URI}
	 * @param servers the servers to set
	 */
	public void setServers(List<URI> servers) {
		this.servers=servers;
	}

	/**
	 * Sets the server list from a comma-delimited list
	 * of server addresses in a single string.
	 * @param urlString the servers to set
	 */
	public void setServers(String urlString) {
		String[] servers = urlString.trim().split("\\s*,\\s*");
		this.setServers(servers);
		System.err.println("Servers = " + getServers());
	}
	
	/**
	 * Sets the server list from a list of {@code String}
	 * @param servers the servers to set
	 * @throws IllegalArgumentException if any of the {@code URI}s
	 * are malformed
	 */
	public void setServers(String[] servers) {
		if (servers==null) 
			this.servers=null;
		else
		{
			if (this.servers == null) {
				this.servers = new ArrayList<URI>();
			}
			this.servers.clear();
			for (String s : servers) {
				try {
					this.servers.add(new URI(s.trim()));
				} catch (URISyntaxException e) {
					throw new IllegalArgumentException(e);
				}
			}
		} 
	}

	/**
	 * @return the noRandomize
	 */
	/**
	 * Indicates whether server list randomization is disabled
	 * <p>
	 * {@code true} means that the server list will be traversed 
	 * in the order in which it was received
	 * <p>
	 * {@code false} means that the server list will be randomized
	 * before it is traversed
	 * @return {@code true} if server list randomization is disabled,
	 * otherwise {@code false}
	 */
	public boolean isNoRandomize() {
		return noRandomize;
	}

	/**
	 * Disables or enables server list randomization
	 * @param noRandomize the noRandomize to set
	 */
	public void setNoRandomize(boolean noRandomize) {
		this.noRandomize=noRandomize;
	}

	/**
	 * Gets the name associated with this Connection
	 * @return the name associated with this Connection.
	 */
	public String getConnectionName() {
		return this.connectionName;
	}

	/**
	 * Sets the name associated with this Connection
	 * @param connectionName the name to set.
	 */
	public void setConnectionName(String connectionName) {
		this.connectionName=connectionName;
	}

	/**
	 * Indicates whether {@code verbose} is set
	 * <p> When {@code verbose==true}, the server will acknowledge each
	 * protocol line with {@code +OK or -ERR}
	 * @return whether {@code verbose} is set
	 */
	public boolean isVerbose() {
		return this.verbose;
	}

	/**
	 * Sets whether {@code verbose} is set
	 * @param verbose whether or not this connection should 
	 * require protocol acks from the server (+OK/-ERR)
	 */
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	/**
	 * Indicates whether strict server-side protocol checking 
	 * is enabled
	 * @return whether {@code pedantic} is set
	 */
	public boolean isPedantic() {
		return this.pedantic;
	}

	/**
	 * Sets whether strict server-side protocol checking 
	 * is enabled. 
	 * <p>
	 * When {@code pedantic==true}, strict 
	 * server-side protocol checking occurs.
	 * @param pedantic whether or not this connection should
	 * require strict server-side protocol checking
	 */
	public void setPedantic(boolean pedantic) {
		this.pedantic = pedantic;
	}

	/**
	 * Indicates whether to require a secure connection with the 
	 * NATS server
	 * @return {@code true} if secure is required, otherwise {@code false}
	 */
	public boolean isSecure() {
		return this.secure;
	}

	/**
	 * Sets whether to require a secure connection with the 
	 * NATS server
	 * @param secure whether to require a secure connection with the 
	 * NATS server
	 */
	public void setSecure(boolean secure) {
		this.secure = secure;
	}

	/**
	 * Indicates whether TLS debug output should be enabled
	 * @return {@code true} if TLS debug is enabled, otherwise {@code false}
	 */
	public boolean isTlsDebug() {
		return tlsDebug;
	}
	
	/**
	 * Sets whether TLS debug output should be enabled
	 * @param debug whether TLS debug output should be enabled
	 */
	public void setTlsDebug(boolean debug) {
		this.tlsDebug = debug;
	}

	/**
	 * Indicates whether reconnection is enabled
	 * @return {@code true} if reconnection is allowed, otherwise 
	 * {@code false} 
	 */
	public boolean isReconnectAllowed() {
		return this.reconnectAllowed;
	}

	/**
	 * Sets whether reconnection is enabled
	 * @param reconnectAllowed whether to allow reconnects
	 */
	public void setReconnectAllowed(boolean reconnectAllowed) {
		this.reconnectAllowed = reconnectAllowed;
	}

	/**
	 * Gets the maximum number of reconnection attempts for this
	 * connection
	 * @return the maximum number of reconnection attempts
	 */
	public int getMaxReconnect() {
		return this.maxReconnect;
	}

	/**
	 * Sets the maximum number of reconnection attempts for this 
	 * connection
	 * @param max the maximum number of reconnection attempts
	 */
	public void setMaxReconnect(int max) {
		this.maxReconnect = max;
	}

	/**
	 * Returns the reconnect wait interval in milliseconds. This is the
	 * amount of time to wait before attempting reconnection to the 
	 * current server
	 * @return the reconnect wait interval in milliseconds
	 */
	public long getReconnectWait() {
		return this.reconnectWait;
	}

	/**
	 * Sets the reconnect wait interval in milliseconds. This is the
	 * amount of time to wait before attempting reconnection to the 
	 * current server
	 * @param interval the reconnectWait to set
	 */
	public void setReconnectWait(long interval) {
		this.reconnectWait = interval;
	}

	/**
	 * Returns the connection timeout interval in milliseconds. This
	 * is the maximum amount of time to wait for a connection to a
	 * NATS server to complete successfully
	 * @return the connection timeout
	 */
	public int getConnectionTimeout() {
		return this.connectionTimeout;
	}

	/**
	 * Sets the connection timeout interval in milliseconds. This
	 * is the maximum amount of time to wait for a connection to a
	 * NATS server to complete successfully
	 * @param timeout the connection timeout
	 * @throws IllegalArgumentException if {@code timeout < 0}
	 */
	public void setConnectionTimeout(int timeout) {
		if(timeout < 0) {
			throw new IllegalArgumentException("TCP connection timeout cannot be negative");
		}
		this.connectionTimeout = timeout;
	}

	/**
	 * Gets the server ping interval in milliseconds. The connection 
	 * will send a PING to the server at this interval to ensure the 
	 * server is still alive
	 * @return the pingInterval
	 */
	public long getPingInterval() {
		return this.pingInterval;
	}

	/**
	 * Sets the server ping interval in milliseconds. The connection 
	 * will send a PING to the server at this interval to ensure the 
	 * server is still alive
	 * @param interval the ping interval to set in milliseconds
	 */
	public void setPingInterval(long interval) {
		this.pingInterval = interval;
	}

	/**
	 * Returns the maximum number of outstanding server pings
	 * @return the maximum number of oustanding outbound pings before 
	 * marking the Connection stale and triggering reconnection
	 * (if allowed).
	 */
	public int getMaxPingsOut() {
		return this.maxPingsOut;
	}

	/**
	 * Sets the maximum number of outstanding pings (pings for which 
	 * no pong has been received). Once this 
	 * limit is exceeded, the connection is marked as stale and closed. 
	 * @param max the maximum number of outstanding pings
	 */
	public void setMaxPingsOut(int max) {
		this.maxPingsOut = max;
	}

	/**
	 * Returns the {@link ClosedCallback}, if one is registered
	 * @return the {@link ClosedCallback}, if one is registered
	 */
	public ClosedCallback getClosedCallback() {
		return closedCallback;
	}

	/**
	 * Sets the {@link ClosedCallback}
	 * @param cb the {@link ClosedCallback} to set
	 */
	public void setClosedCallback(ClosedCallback cb) {
		this.closedCallback = cb;
	}

	/**
	 * Returns the {@link DisconnectedCallback}, if one is registered
	 * @return the {@link DisconnectedCallback}, if one is registered
	 */
	public DisconnectedCallback getDisconnectedCallback() {
		return disconnectedCallback;
	}

	/**
	 * Sets the {@link DisconnectedCallback}
	 * @param cb the {@link DisconnectedCallback} to set
	 */
	public void setDisconnectedCallback(DisconnectedCallback cb) {
		this.disconnectedCallback = cb;
	}

	/**
	 * Returns the {@link ReconnectedCallback}, if one is registered
	 * @return the {@link ReconnectedCallback}, if one is registered
	 *
	 */
	public ReconnectedCallback getReconnectedCallback() {
		return reconnectedCallback;
	}

	/**
	 * Sets the {@link ReconnectedCallback}
	 * @param cb the {@link ReconnectedCallback} to set
	 */
	public void setReconnectedCallback(ReconnectedCallback cb) {
		this.reconnectedCallback = cb;
	}

	/**
	 * Returns the {@link ExceptionHandler}, if one is registered
	 * @return the {@link ExceptionHandler}, if one is registered
	 *
	 */
	public ExceptionHandler getExceptionHandler() {
		return exceptionHandler;
	}

	/**
	 * Sets the {@link ExceptionHandler}
	 * @param exceptionHandler the {@link ExceptionHandler} to set for 
	 *        connection.
	 */
	public void setExceptionHandler(ExceptionHandler exceptionHandler) {
		if (exceptionHandler == null) {
			throw new IllegalArgumentException("ExceptionHandler cannot be null!");
		}
		this.exceptionHandler = exceptionHandler;
	}

	/**
	 * 
	 * @deprecated use {@link #getSSLContext} instead.
	 * 
	 * Returns the {@link SSLContext} for this connection factory
	 * @return the {@link SSLContext} for this connection factory
	 */
	@Deprecated
	public SSLContext getSslContext() {
		return sslContext;
	}

	/**
	 * 
	 * Returns the {@link SSLContext} for this connection factory
	 * @return the {@link SSLContext} for this connection factory
	 */
	public SSLContext getSSLContext() {
		return sslContext;
	}
	
	/**
	 * 
	 * @deprecated use {@link #setSSLContext} instead.
	 * 
	 * Sets the {@link SSLContext} for this connection factory
	 * @param ctx the {@link SSLContext} to set
	 * 
	 */
	@Deprecated
	public void setSslContext(SSLContext ctx) {
		setSSLContext(ctx);
	}
	
	/**
	 * Sets the {@link SSLContext} for this connection factory
	 * @param ctx the {@link SSLContext} to set
	 * 
	 */	
	public void setSSLContext(SSLContext ctx) {
		this.sslContext = ctx;
	}
}
