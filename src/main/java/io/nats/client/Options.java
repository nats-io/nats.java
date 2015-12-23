package io.nats.client;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.List;

import javax.net.ssl.SSLContext;

public class Options {
	private URI url;
	private String host;
	private int port;
	private String username;
	private String password;
	private List<URI> servers;	
	private boolean noRandomize;
	private String connectionName;
	private boolean verbose;
	private boolean pedantic;
	private boolean secure;
	private boolean reconnectAllowed;
	private int maxReconnect;
	private long reconnectWait;
	private int connectionTimeout;
	private long pingInterval;
	private int maxPingsOut;
	private ExceptionHandler exceptionHandler;
	private SSLContext sslContext;
	
//	private List<X509Certificate> certificates =
//			new ArrayList<X509Certificate>();
	
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
	private int subChanLen;
	public DisconnectedEventHandler disconnectedEventHandler;
	public ClosedEventHandler closedEventHandler;
	public ReconnectedEventHandler reconnectedEventHandler;
	public ExceptionHandler asyncErrorEventHandler;
	
	public URI getUrl() {
		return url;
	}	
	public void setUrl(URI url) {
		this.url = url;
		if (url != null) {
			if (url.getHost()!=null) {
				this.setHost(url.getHost());
			}
			this.setPort(url.getPort());
			
	        String userInfo = url.getRawUserInfo();
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
	public void setUrl(String url) {
		try {
			if ((url != null) && !url.isEmpty())
			this.url = new URI(url);
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException("Bad server URL: " + url);
		}
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public List<URI> getServers() {
		return servers;
	}
	public void setServers(String[] servers) {
		if (servers != null) {
			for (String s : servers) {
				if (s!=null && !s.isEmpty())
				{
					try {
						this.servers.add(new URI(s));						
					} catch (URISyntaxException e) {
						throw new IllegalArgumentException("Bad server URL: " + s);
					}
				}
			}
		}
	}
	public void setServers(List<URI> servers) {
		this.servers = servers;
	}
	public boolean isNoRandomize() {
		return noRandomize;
	}
	public void setNoRandomize(boolean randomizeDisabled) {
		this.noRandomize = randomizeDisabled;
	}
	public String getConnectionName() {
		return connectionName;
	}
	public void setConnectionName(String connectionName) {
		this.connectionName = connectionName;
	}
	public boolean isVerbose() {
		return verbose;
	}
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}
	public boolean isPedantic() {
		return pedantic;
	}
	public void setPedantic(boolean pedantic) {
		this.pedantic = pedantic;
	}
	public boolean isSecure() {
		return secure;
	}
	public void setSecure(boolean secure) {
		this.secure = secure;
	}
	public boolean isReconnectAllowed() {
		return reconnectAllowed;
	}
	public void setReconnectAllowed(boolean reconnectAllowed) {
		this.reconnectAllowed = reconnectAllowed;
	}
	public int getMaxReconnect() {
		return maxReconnect;
	}
	public void setMaxReconnect(int maxReconnect) {
		this.maxReconnect = maxReconnect;
	}
	public long getReconnectWait() {
		return reconnectWait;
	}
	public void setReconnectWait(long reconnectWait) {
		this.reconnectWait = reconnectWait;
	}
	public int getConnectionTimeout() {
		return connectionTimeout;
	}
	public void setConnectionTimeout(int connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}
	public long getPingInterval() {
		return pingInterval;
	}
	public void setPingInterval(long pingInterval) {
		this.pingInterval = pingInterval;
	}
	public int getMaxPingsOut() {
		return maxPingsOut;
	}
	public void setMaxPingsOut(int maxPingsOut) {
		this.maxPingsOut = maxPingsOut;
	}
	public ExceptionHandler getExceptionHandler() {
		return exceptionHandler;
	}
	public void setExceptionHandler(ExceptionHandler exceptionHandler) {
		this.exceptionHandler = exceptionHandler;
	}
//	public ConnectionEventHandler getConnectionListener() {
//		return connectionEventHandler;
//	}
//	public void setConnectionListener(ConnectionEventHandler cb) {
//		this.connectionEventHandler = cb;
//	}
	public ClosedEventHandler getClosedEventHandler() {
		return closedEventHandler;
	}
	public void setClosedEventHandler(ClosedEventHandler cb) {
		this.closedEventHandler = cb;
	}
	public ReconnectedEventHandler getReconnectedEventHandler() {
		return reconnectedEventHandler;
	}
	public void setReconnectedEventHandler(ReconnectedEventHandler cb) {
		this.reconnectedEventHandler = cb;
	}
	public DisconnectedEventHandler getDisconnectedEventHandler() {
		return disconnectedEventHandler;
	}
	public void setDisconnectedEventHandler(DisconnectedEventHandler cb) {
		this.disconnectedEventHandler = cb;
	}
	
//	public void addCertificate(X509Certificate cert) {
//		if (cert==null)
//			throw new IllegalArgumentException("Null certificate");
//		certificates.add(cert);
//	}
//	
//	public void addCertificate(byte[] cert) throws CertificateException {
//		if (cert==null)
//			throw new IllegalArgumentException("Null certificate");
//		CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
//		InputStream in = new ByteArrayInputStream(cert);
//		X509Certificate theCert = (X509Certificate)certFactory.generateCertificate(in);
//		certificates.add(theCert);
//	}
//	
//	public void addCertificate(String cert) throws CertificateException {
//		addCertificate(cert.getBytes(Charset.forName("UTF-8")));
//	}
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
