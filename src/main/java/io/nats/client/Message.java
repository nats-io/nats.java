/**
 * 
 */
package io.nats.client;

import java.util.Arrays;

/**
 * @author Larry McQueary
 *
 */
public final class Message {
	private String subject;
	private String replyTo;
	private byte[] data;  
	protected SubscriptionImpl sub;

	public Message() {
        this.subject = null;
        this.replyTo = null;
        this.data    = null;
        this.sub     = null;

	}
	
	public Message(String subject, String reply, byte[] data)
    {
		this(data, (null!=data ? data.length : 0), subject, reply, null);
    }
	
	protected Message(MsgArg msgArgs, SubscriptionImpl sub, byte[] data, long length) {
		this(data, length, msgArgs.subject, msgArgs.reply, sub);        
	}

	protected Message(byte[] data, long length, String subject, String reply, SubscriptionImpl sub)
	{
        if (subject==null || subject.trim().length()==0)
        {
            throw new IllegalArgumentException(
                "Subject cannot be null, empty, or whitespace.");
        }
        this.data = new byte[(int) length];
        // make a deep copy of the bytes for this message.
        if (data !=null)
        	System.arraycopy(data, 0, this.data, 0, (int)length);
        this.subject = subject;
        this.replyTo = reply;
        this.sub = sub;
	}
	
	public byte[] getData() {
		return data;
	}
	
	public String getSubject() {
		return subject;
	}
	
	public Message setSubject(String subject) {
		this.subject = subject;
		return this;
	}
	
	public String getReplyTo() {
		return this.replyTo;
	}
	
	public Message setReplyTo(String replyTo) {
		this.replyTo = replyTo;
		return this;
	}
	
	public Subscription getSubscription() {
		return this.sub;
	}
	
	public Message setData(byte[] data, int offset, int len) {
		if (data == null) {
			this.data = null;
		} else {
			this.data = new byte[len];
			this.data = Arrays.copyOfRange(data, offset, len);
		}
		return this;
	}
	
	/**
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 * @return a string reprsenetation of the message
	 */
	@Override
    public String toString()
    {
		int maxBytes = 32;
		byte[] b = getData();
		int len = b.length;

		StringBuilder sb = new StringBuilder();
		sb.append(String.format(
				"{Subject=%s;Reply=%s;Payload=<", 
				getSubject(), getReplyTo()));
		
		for (int i=0; i<maxBytes && i<len; i++) {
			sb.append((char)b[i]);
		}

//		int remainder = maxBytes - len;
		int remainder = len - maxBytes;
		if (remainder > 0) {
            sb.append(String.format("%d more bytes", remainder));
		}
		
		sb.append(">}");

        return sb.toString();
    }
}
