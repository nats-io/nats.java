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
 * Factpry class for opening a {@link ConnectionImpl} to the NATS server (gnatsd). 
 */

public class ConnectionFactory implements Cloneable {

	// Default request channel length
	// protected static final int		REQUEST_CHAN_LEN		= 4;
	// Default server pool size
	// protected static final int DEFAULT_SERVER_POOL_SIZE = 4;

	private URI url									= null;
	private String host								= Constants.DEFAULT_HOST;
	private int port								= Constants.DEFAULT_PORT;
	private String username							= null;
	private String password							= null;
	private List<URI> servers						= new ArrayList<URI>();	
	private boolean noRandomize						= false;
	private String connectionName					= null;
	private boolean verbose							= false;
	private boolean pedantic						= false;
	private boolean secure							= false;
	private boolean reconnectAllowed				= true;
	private int maxReconnect						= Constants.DEFAULT_MAX_RECONNECT;
	private long reconnectWait						= Constants.DEFAULT_RECONNECT_WAIT;
	private int connectionTimeout					= Constants.DEFAULT_TIMEOUT;
	private long pingInterval						= Constants.DEFAULT_PING_INTERVAL;
	private int maxPingsOut							= Constants.DEFAULT_MAX_PINGS_OUT;
	private SSLContext sslContext;
	private ExceptionHandler exceptionHandler;
	private ClosedEventHandler closedEventHandler;
	private DisconnectedEventHandler disconnectedEventHandler;
	private ReconnectedEventHandler reconnectedEventHandler;

	private String urlString 						= Constants.DEFAULT_URL;

	// The size of the buffered channel used between the socket
	// Go routine and the message delivery or sync subscription.
	private int subChanLen							= Constants.DEFAULT_MAX_CHAN_LEN;

	//	public ConnectionFactory(Properties props) {
	//		
	//	}

	public ConnectionFactory(Properties props) {
		if (props==null)
			throw new IllegalArgumentException("Properties cannot be null");

		//PROP_URL
		if (props.containsKey(PROP_URL))
			this.setUrl(props.getProperty(PROP_URL));
		//PROP_HOST
		if (props.containsKey(PROP_HOST))
			this.setHost(props.getProperty(PROP_HOST, DEFAULT_HOST));
		//PROP_PORT
		if (props.containsKey(PROP_PORT))
			this.setPort(Integer.parseInt(props.getProperty(PROP_PORT, Integer.toString(DEFAULT_PORT))));
		//PROP_USERNAME
		if (props.containsKey(PROP_USERNAME))
			this.setUsername(props.getProperty(PROP_USERNAME, null));
		//PROP_PASSWORD
		if (props.containsKey(PROP_PASSWORD))
			this.setPassword(props.getProperty(PROP_PASSWORD, null));
		//PROP_SERVERS
		if (props.containsKey(PROP_SERVERS)) {
			String s = props.getProperty(PROP_SERVERS);
			if (!(s==null) && !s.isEmpty()) {
				String[] servers = s.trim().split("\\s+|,");
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
			this.setVerbose(Boolean.parseBoolean(props.getProperty(PROP_PEDANTIC)));
		//PROP_SECURE
		if (props.containsKey(PROP_SECURE))
			this.setSecure(Boolean.parseBoolean(props.getProperty(PROP_SECURE)));
		//PROP_RECONNECT_ALLOWED
		if (props.containsKey(PROP_RECONNECT_ALLOWED))
			this.setReconnectAllowed(Boolean.parseBoolean(
					props.getProperty(PROP_RECONNECT_ALLOWED, Boolean.toString(true))));
		//PROP_MAX_RECONNECT
		if (props.containsKey(PROP_MAX_RECONNECT))
			this.setPort(Integer.parseInt(
					props.getProperty(PROP_MAX_RECONNECT, Integer.toString(DEFAULT_MAX_RECONNECT))));
		//PROP_RECONNECT_WAIT
		if (props.containsKey(PROP_RECONNECT_WAIT))
			this.setPort(Integer.parseInt(
					props.getProperty(PROP_RECONNECT_WAIT, Integer.toString(DEFAULT_RECONNECT_WAIT))));
		//PROP_CONNECTION_TIMEOUT
		if (props.containsKey(PROP_CONNECTION_TIMEOUT))
			this.setPort(Integer.parseInt(
					props.getProperty(PROP_CONNECTION_TIMEOUT, Integer.toString(DEFAULT_TIMEOUT))));
		//PROP_PING_INTERVAL
		if (props.containsKey(PROP_PING_INTERVAL))
			this.setPort(Integer.parseInt(
					props.getProperty(PROP_PING_INTERVAL, Integer.toString(DEFAULT_PING_INTERVAL))));
		//PROP_MAX_PINGS
		if (props.containsKey(PROP_MAX_PINGS))
			this.setPort(Integer.parseInt(
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
		//PROP_CLOSED_HANDLER
		if (props.containsKey(PROP_CLOSED_HANDLER)) {
			Object instance = null;
			try {
				String s = props.getProperty(PROP_CLOSED_HANDLER);
				Class<?> clazz = Class.forName(s);
				Constructor<?> constructor = clazz.getConstructor();
				instance = constructor.newInstance();
			} catch (Exception e) {
				throw new IllegalArgumentException(e);
			} finally {}
			this.setClosedEventHandler((ClosedEventHandler) instance);
		}
		//PROP_DISCONNECTED_HANDLER
		if (props.containsKey(PROP_DISCONNECTED_HANDLER)) {
			Object instance = null;
			try {
				String s = props.getProperty(PROP_DISCONNECTED_HANDLER);
				Class<?> clazz = Class.forName(s);
				Constructor<?> constructor = clazz.getConstructor();
				instance = constructor.newInstance();
			} catch (Exception e) {
				throw new IllegalArgumentException(e);
			} finally {}
			this.setDisconnectedEventHandler((DisconnectedEventHandler) instance);
		}

		//PROP_RECONNECTED_HANDLER
	}

	public ConnectionFactory() {
		this(null, null);
	}

	public ConnectionFactory(String url)
	{
		this(url, null);
	}

	public ConnectionFactory(String[] servers) {
		this(null, servers);
	}

	public ConnectionFactory(String url, String[] servers)
	{
		if ((url!=null) && !url.isEmpty())
		{
			this.setUrl(url);
		}

		if ((servers != null) && (servers.length != 0))
		{
			this.servers = new ArrayList<URI>();
			for (String s : servers) {
				try {
					this.getServers().add(new URI(s));
				} catch (URISyntaxException e) {
					// TODO Auto-generated catch block
					throw new IllegalArgumentException(
							"Badly formed server URL: " + s);
				}
			}
		}
		if (this.url==null && this.servers==null) {
			this.setUrl(Constants.DEFAULT_URL);
		}
	}

	/**
	 * 
	 * @return the Connection.
	 * @throws IOException if a Connection cannot be established for some reason.
	 * @throws TimeoutException if the connection timeout has been exceeded.
	 */
	public ConnectionImpl createConnection() throws IOException, TimeoutException {
		ConnectionImpl conn = null;
		Options options = options();

		conn = new ConnectionImpl(options);

		conn.start();

		return conn;
	}

	private Options options() {
		Options result = new Options();
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
		result.setReconnectAllowed(reconnectAllowed);
		result.setMaxReconnect(maxReconnect);
		result.setReconnectWait(reconnectWait);
		result.setConnectionTimeout(connectionTimeout);
		result.setPingInterval(pingInterval);
		result.setMaxPingsOut(maxPingsOut);
		result.setExceptionHandler(exceptionHandler);
		result.setClosedEventHandler(closedEventHandler);
		result.setDisconnectedEventHandler(disconnectedEventHandler);
		result.setReconnectedEventHandler(reconnectedEventHandler);
		result.setSubChanLen(subChanLen);
		//    	private ConnectionEventHandler connectionEventHandler 	= null;		return null;
		result.setSslContext(sslContext);
		return result;
	}

	/**
	 * @return the subChanLen
	 */
	public int getSubChanLen() {
		return subChanLen;
	}

	/**
	 * @param subChanLen the subChanLen to set
	 */
	public void setSubChanLen(int subChanLen) {
		this.subChanLen = subChanLen;
	}

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

		String userInfo = uri.getRawUserInfo();
		if (userInfo != null) {
			String userPass[] = userInfo.split(":");
			if (userPass.length > 2) {
				throw new IllegalArgumentException("Bad user info in NATS " +
						"URI: " + userInfo);
			}

			setUsername(uriDecode(userPass[0]));
			if (userPass.length == 2) {
				setPassword(uriDecode(userPass[1]));
			}
		}
	}

	private String uriDecode(String s) {
		try {
			// URLDecode decodes '+' to a space, as for
			// form encoding.  So protect plus signs.
			return URLDecoder.decode(s.replace("+", "%2B"), "US-ASCII");
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * @return the url
	 */
	public String getUrlString() {
		return this.urlString;
	}

	/**
	 * @param url the url to set
	 */
	public void setUrl(String url) {
		this.urlString=url;
		try {
			this.setUri(new URI(url));
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * @return the host
	 */
	public String getHost() {
		return this.host;
	}

	/**
	 * @param host the host to set
	 */
	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * @return the port
	 */
	public int getPort() {
		return this.port;
	}

	/**
	 * @param port the port to set
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * @return the username
	 */
	public String getUsername() {
		return this.username;
	}

	/**
	 * @param username the username to set
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * @return the password
	 */
	public String getPassword() {
		return this.password;
	}

	/**
	 * @param password the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * @return the servers
	 */
	public List<URI> getServers() {
		return this.servers;
	}

	/**
	 * @param servers the servers to set
	 */
	public void setServers(List<URI> servers) {
		this.servers=servers;
	}

	/**
	 * @param servers the servers to set
	 */
	public void setServers(String[] servers) {
		if (servers==null) 
			this.servers=null;
		else
		{
			this.servers.clear();
			for (String s : servers) {
				try {
					this.servers.add(new URI(s));
				} catch (URISyntaxException e) {
					throw new IllegalArgumentException(e);
				}
			}
		} 
	}

	/**
	 * @return the noRandomize
	 */
	public boolean isNoRandomize() {
		return noRandomize;
	}

	/**
	 * @param noRandomize the noRandomize to set
	 */
	public void setNoRandomize(boolean noRandomize) {
		this.noRandomize=noRandomize;
	}

	/**
	 * @return the connection name associated with this Connection.
	 */
	public String getConnectionName() {
		return this.connectionName;
	}

	/**
	 * @param connectionName the name to set for this Connection.
	 */
	public void setConnectionName(String connectionName) {
		this.connectionName=connectionName;
	}

	/**
	 * @return whether or not the connection will require +OK/+ERR
	 */
	public boolean isVerbose() {
		return this.verbose;
	}

	/**
	 * @param verbose whether or not this Connection should 
	 * require protocol acks from the server (+OK/-ERR)
	 */
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	/**
	 * @return whether strict server-side protocol checking 
	 * 		   is enabled
	 */
	public boolean isPedantic() {
		return this.pedantic;
	}

	/**
	 * @param pedantic when <code>true</code>, strict 
	 * 				   server-side protocol checking occurs.
	 */
	public void setPedantic(boolean pedantic) {
		this.pedantic = pedantic;
	}

	/**
	 * @return the secure
	 */
	public boolean isSecure() {
		return this.secure;
	}

	/**
	 * @param secure the secure to set
	 */
	public void setSecure(boolean secure) {
		this.secure = secure;
	}

	/**
	 * @return the reconnectAllowed
	 */
	public boolean isReconnectAllowed() {
		return this.reconnectAllowed;
	}

	/**
	 * @param reconnectAllowed the reconnectAllowed to set
	 */
	public void setReconnectAllowed(boolean reconnectAllowed) {
		this.reconnectAllowed = reconnectAllowed;
	}

	/**
	 * @return the maxReconnect
	 */
	public int getMaxReconnect() {
		return this.maxReconnect;
	}

	/**
	 * @param maxReconnect the maxReconnect to set
	 */
	public void setMaxReconnect(int maxReconnect) {
		this.maxReconnect = maxReconnect;
	}

	/**
	 * @return the reconnectWait
	 */
	public long getReconnectWait() {
		return this.reconnectWait;
	}

	/**
	 * @param reconnectWait the reconnectWait to set
	 */
	public void setReconnectWait(long reconnectWait) {
		this.reconnectWait = reconnectWait;
	}

	/**
	 * @return the connectionTimeout
	 */
	public int getConnectionTimeout() {
		return this.connectionTimeout;
	}

	/**
	 * @param connectionTimeout the connectionTimeout to set
	 */
	public void setConnectionTimeout(int connectionTimeout) {
		if(connectionTimeout < 0) {
			throw new IllegalArgumentException("TCP connection timeout cannot be negative");
		}
		this.connectionTimeout = connectionTimeout;
	}

	/**
	 * @return the pingInterval
	 */
	public long getPingInterval() {
		return this.pingInterval;
	}

	/**
	 * @param pingInterval the pingInterval to set
	 */
	public void setPingInterval(long pingInterval) {
		this.pingInterval = pingInterval;
	}

	/**
	 * @return the maximum number of oustanding outbound pings before 
	 * 		   marking the Connection stale and triggering reconnection.
	 */
	public int getMaxPingsOut() {
		return this.maxPingsOut;
	}

	/**
	 * @return the closedEventHandler
	 */
	public ClosedEventHandler getClosedEventHandler() {
		return closedEventHandler;
	}

	/**
	 * @param closedEventHandler the closedEventHandler to set
	 */
	public void setClosedEventHandler(ClosedEventHandler closedEventHandler) {
		this.closedEventHandler = closedEventHandler;
	}

	/**
	 * @return the disconnectedEventHandler
	 */
	public DisconnectedEventHandler getDisconnectedEventHandler() {
		return disconnectedEventHandler;
	}

	/**
	 * @param disconnectedEventHandler the disconnectedEventHandler to set
	 */
	public void setDisconnectedEventHandler(DisconnectedEventHandler disconnectedEventHandler) {
		this.disconnectedEventHandler = disconnectedEventHandler;
	}

	/**
	 * @return the reconnectedEventHandler
	 */
	public ReconnectedEventHandler getReconnectedEventHandler() {
		return reconnectedEventHandler;
	}

	/**
	 * @param reconnectedEventHandler the reconnectedEventHandler to set
	 */
	public void setReconnectedEventHandler(ReconnectedEventHandler reconnectedEventHandler) {
		this.reconnectedEventHandler = reconnectedEventHandler;
	}

	/**
	 * @param maxPingsOut the maxPingsOut to set
	 */
	public void setMaxPingsOut(int maxPingsOut) {
		this.maxPingsOut = maxPingsOut;
	}

	/**
	 * @return the exceptionHandler
	 */
	public ExceptionHandler getExceptionHandler() {
		return exceptionHandler;
	}

	/**
	 * @param exceptionHandler the {@link ExceptionHandler} to set for 
	 *        connections.
	 */
	public void setExceptionHandler(ExceptionHandler exceptionHandler) {
		if (exceptionHandler == null) {
			throw new IllegalArgumentException("ExceptionHandler cannot be null!");
		}
		this.exceptionHandler = exceptionHandler;
	}

	/**
	 * @return the sslContext
	 */
	public SSLContext getSslContext() {
		return sslContext;
	}

	/**
	 * @param sslContext the sslContext to set
	 */
	public void setSslContext(SSLContext sslContext) {
		this.sslContext = sslContext;
	}
}
