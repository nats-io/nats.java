package io.nats.client;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

import io.nats.client.impl.NATSConnection;

/* *
 * Factpry class for opening a {@link NATSConnection} to the NATS server (gnatsd). 
 */

public class ConnectionFactory implements Cloneable {

	// Default server host
	protected static final String	DEFAULT_HOST			= "localhost";
	// Default server port
	protected static final int 		DEFAULT_PORT			= 4222;
	// Default server URL
	protected static final String 	DEFAULT_URL				= 
			"nats://" + DEFAULT_HOST+ ":" + DEFAULT_PORT;
	// Default maximum number of reconnect attempts.
	protected static final int		DEFAULT_MAX_RECONNECT	= 60;
	// Default wait time before attempting reconnection to the same server
	protected static final int		DEFAULT_RECONNECT_WAIT	= 2 * 1000;
	// Default connection timeout
	protected static final int		DEFAULT_TIMEOUT			= 2 * 1000;
	// Default ping interval; <=0 means disabled
	protected static final int 		DEFAULT_PING_INTERVAL	= 2 * 60000;
	// Default maximum number of pings that have not received a response
	protected static final int		DEFAULT_MAX_PINGS_OUT	= 2;
	// Default maximum channel length
	protected static final int		DEFAULT_MAX_CHAN_LEN	= 65536;
	// Default request channel length
	// protected static final int		REQUEST_CHAN_LEN		= 4;
	// Default server pool size
	// protected static final int DEFAULT_SERVER_POOL_SIZE = 4;
	
	private String username							= null;
	private String password							= null;
	private String host								= ConnectionFactory.DEFAULT_HOST;
	private int port								= ConnectionFactory.DEFAULT_PORT;
	private String urlString 						= ConnectionFactory.DEFAULT_URL;
	private URI url									= null;
	private List<URI> servers						= null;	
	private boolean noRandomize						= false;
	private String connectionName					= null;
	private boolean verbose							= false;
	private boolean pedantic						= false;
	private boolean secure							= false;
	private boolean reconnectAllowed				= false;
	private int maxReconnect						= ConnectionFactory.DEFAULT_MAX_RECONNECT;
	private long reconnectWait						= ConnectionFactory.DEFAULT_RECONNECT_WAIT;
	private int connectionTimeout					= ConnectionFactory.DEFAULT_TIMEOUT;
	private long pingInterval						= ConnectionFactory.DEFAULT_PING_INTERVAL;
	private int maxPingsOut							= ConnectionFactory.DEFAULT_MAX_PINGS_OUT;
	private ExceptionHandler exceptionHandler 		= new DefaultExceptionHandler();
	private ConnEventHandler connEventHandler 	= null;
	
	// The size of the buffered channel used between the socket
	// Go routine and the message delivery or sync subscription.
	private int subChanLen							= DEFAULT_MAX_CHAN_LEN;
	
//	public ConnectionFactory(Properties props) {
//		
//	}
	
	public ConnectionFactory() {
		new ConnectionFactory(DEFAULT_URL, null);
	}

	public ConnectionFactory(String url)
	{
		new ConnectionFactory(url, null);
	}
	
	public ConnectionFactory(String[] servers) {
		new ConnectionFactory(null, servers);
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
			for (String s : servers)
				try {
					this.getServers().add(new URI(s));
				} catch (URISyntaxException e) {
					// TODO Auto-generated catch block
					throw new IllegalArgumentException(
							"Badly formed server URL: " + s);
				}
		}
		if (this.url==null && this.servers==null) {
			try {
				this.url = new URI(DEFAULT_URL);
			} catch (URISyntaxException e) {
				// This signifies programmer error
			}
		}
	}
	
	public NATSConnection createConnection() throws IOException, NATSException {
		NATSConnection conn = null;
		Options options = options();
		
		try {
			conn = new NATSConnection(options);
		} catch (NoServersException e) {
			throw(new IOException(e));
		}
		
		try {
			conn.connect();
		} catch (Exception e) {
			throw new NATSException(e);
		}
		
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
    	result.setSubChanLen(subChanLen);
//    	private ConnEventHandler connEventHandler 	= null;		return null;
    	return result;
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
	public void setUrl(String urlString) {
		this.urlString=urlString;
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
	 * @return the connectionName
	 */
	public String getConnectionName() {
		return this.connectionName;
	}

	/**
	 * @param connectionName the connectionName to set
	 */
	public void setConnectionName(String connectionName) {
		this.connectionName=connectionName;
	}

	/**
	 * @return the verbose
	 */
	public boolean isVerbose() {
		return this.verbose;
	}

	/**
	 * @param verbose the verbose to set
	 */
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	/**
	 * @return the pedantic
	 */
	public boolean isPedantic() {
		return this.pedantic;
	}

	/**
	 * @param pedantic the pedantic to set
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
	 * @return the maxPingsOut
	 */
	public int getMaxPingsOut() {
		return this.maxPingsOut;
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
	 * @param exceptionHandler the exceptionHandler to set
	 */
	public void setExceptionHandler(ExceptionHandler exceptionHandler) {
		if (exceptionHandler == null) {
			throw new IllegalArgumentException("ExceptionHandler cannot be null!");
		}
		this.exceptionHandler = exceptionHandler;
	}
}
