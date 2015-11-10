package io.nats.client;

public interface Message {
	public byte[] getData();
	public Message setData(byte[] data, int offset, int len);
	public String getSubject();
	public Message setSubject(String subject);
	public String getReplyTo();
	public Message setReplyTo(String subject);
	public Subscription getSubscription();
	
}
