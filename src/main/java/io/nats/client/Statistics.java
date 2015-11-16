package io.nats.client;

import java.util.concurrent.atomic.AtomicLong;

// Tracks various stats received and sent on this connection,
// including counts for messages and bytes.
public class Statistics implements Cloneable {

	private AtomicLong inMsgs = new AtomicLong();
	private AtomicLong outMsgs = new AtomicLong();
	private AtomicLong inBytes = new AtomicLong();
	private AtomicLong outBytes = new AtomicLong();
	private AtomicLong reconnects = new AtomicLong();
	
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
		this.inBytes.set(0L);
		this.inMsgs.set(0L);;
		this.outBytes.set(0L);;
		this.outMsgs.set(0L);

	}

	/**
	 * @return the inMsgs
	 */
	public synchronized long getInMsgs() {
		return inMsgs.get();
	}

	/**
	 * 
	 */
	public synchronized long incrementInMsgs() {
		return inMsgs.incrementAndGet();
	}

	/**
	 * @return the outMsgs
	 */
	public synchronized long getOutMsgs() {
		return outMsgs.get();
	}
	/**
	 * 
	 */
	public synchronized long incrementOutMsgs() {
		return outMsgs.incrementAndGet();
	}

	/**
	 * @return the inBytes
	 */
	public synchronized long getInBytes() {
		return inBytes.get();
	}
	
	/**
	 * 
	 */
	public synchronized long incrementInBytes(long amount) {
		return inBytes.addAndGet(amount);
	}

	/**
	 * @return the outBytes
	 */
	public synchronized long getOutBytes() {
		return outBytes.get();
	}
	/**
	 * 
	 */
	public synchronized long incrementOutBytes(long delta) {
		return outBytes.addAndGet(delta);
	}

	/**
	 * @return the reconnects
	 */
	public synchronized long getReconnects() {
		return reconnects.get();
	}
	
	public synchronized long incrementReconnects() {
		return reconnects.incrementAndGet();
	}

}

