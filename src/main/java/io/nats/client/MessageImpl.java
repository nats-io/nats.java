/**
 * 
 */
package io.nats.client;

import java.util.Arrays;

import io.nats.client.Parser.MsgArg;

/**
 * @author Larry McQueary
 *
 */
public class MessageImpl implements Message {
	protected MessageImpl() {
		
	}
	public MessageImpl(MsgArg msgArgs, SubscriptionImpl s, byte[] msg, long length) {
		// TODO Auto-generated constructor stub
	}
	private String subject;
	private String replyTo;
	private byte[] data;  
	protected SubscriptionImpl sub;

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
		return this.getReplyTo();
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
				"{Subject=%s;Reply=%s;Payload=<%s", 
				getSubject(), getReplyTo(),
				(b.length <= maxBytes) ? b.toString() : new String(b, 0, maxBytes-1)));

		if (b.length > maxBytes) {
            sb.append(String.format("%d more bytes", len-maxBytes));
		}
		
		sb.append(">}");

        return sb.toString();
    }
}
