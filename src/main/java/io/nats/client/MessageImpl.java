/**
 * 
 */
package io.nats.client;

import java.util.Arrays;

/**
 * @author Larry McQueary
 *
 */
final class MessageImpl implements Message {
	private String subject;
	private String replyTo;
	private byte[] data;  
	protected SubscriptionImpl sub;

	protected MessageImpl() {
        this.subject = null;
        this.replyTo = null;
        this.data    = null;
        this.sub     = null;

	}
	
	protected MessageImpl(String subject, String reply, byte[] data)
    {
        if (subject==null || subject.trim().length()==0)
        {
            throw new IllegalArgumentException(
                "Subject cannot be null, empty, or whitespace.");
        }

        this.subject = subject;
        this.replyTo = reply;
        this.data = data;
    }
	
	public MessageImpl(MsgArg msgArgs, SubscriptionImpl sub, byte[] msg, long length) {
		this.subject = msgArgs.subject;
		this.replyTo = msgArgs.reply;
		this.sub = sub;
        // make a deep copy of the bytes for this message.
        this.data = new byte[(int) length];
        System.arraycopy(msg, 0, this.data, 0, (int)length);
	}

	@Override
	public byte[] getData() {
		return data;
	}
	
	@Override
	public String getSubject() {
		return subject;
	}
	
	@Override
	public Message setSubject(String subject) {
		this.subject = subject;
		return this;
	}
	
	@Override
	public String getReplyTo() {
		return this.replyTo;
	}
	
	@Override
	public Message setReplyTo(String replyTo) {
		this.replyTo = replyTo;
		return this;
	}
	
	@Override
	public io.nats.client.Subscription getSubscription() {
		// TODO Auto-generated method stub
		return this.sub;
	}
	
	@Override
	public Message setData(byte[] data, int offset, int len) {
		this.data = new byte[len];
		if (data != null)
			this.data = Arrays.copyOfRange(data, offset, len);
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
