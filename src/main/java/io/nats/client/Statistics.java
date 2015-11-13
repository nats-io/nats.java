package io.nats.client;

// Tracks various stats received and sent on this connection,
// including counts for messages and bytes.
public class Statistics implements Cloneable {

	long inMsgs = 0;
	long outMsgs = 0;
	long inBytes = 0;
	long outBytes = 0;
	long reconnects = 0;
	
	Statistics () {
		
	}
	
    // deep copy contructor
    Statistics(Statistics obj)
    {
        this.inMsgs = obj.inMsgs;
        this.inBytes = obj.inBytes;
        this.outBytes = obj.outBytes;
        this.outMsgs = obj.outMsgs;
        this.reconnects = obj.reconnects;
    }

	@Override
	protected Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

	public void clear() {
		this.inBytes  = 0;
		this.inMsgs   = 0;
		this.outBytes = 0;
		this.outMsgs  = 0;

	}

	/**
	 * @return the inMsgs
	 */
	public long getInMsgs() {
		return inMsgs;
	}
	/**
	 * @return the outMsgs
	 */
	public long getOutMsgs() {
		return outMsgs;
	}
	/**
	 * @return the inBytes
	 */
	public long getInBytes() {
		return inBytes;
	}
	/**
	 * @return the outBytes
	 */
	public long getOutBytes() {
		return outBytes;
	}
	/**
	 * @return the reconnects
	 */
	public long getReconnects() {
		return reconnects;
	}

}

