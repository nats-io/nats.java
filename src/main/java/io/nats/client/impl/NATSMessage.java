/**
 * 
 */
package io.nats.client.impl;

import java.util.Arrays;

import io.nats.client.Message;
import io.nats.client.impl.Parser.MsgArg;

/**
 * @author Larry McQueary
 *
 */
public class NATSMessage implements Message {
	protected NATSMessage() {
		
	}
	public NATSMessage(MsgArg msgArgs, NATSSubscription s, byte[] msg, long length) {
		// TODO Auto-generated constructor stub
	}
	private String subject;
	private String replyTo;
	private byte[] data;  
	protected NATSSubscription sub;

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
}
