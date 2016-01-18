/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
/**
 * 
 */
package io.nats.client;

import java.util.Arrays;

/**
 * A {@code Message} object is used to send a message containing a 
 * stream of uninterpreted bytes.
 */
public class Message {
	private String subject;
	private String replyTo;
	private byte[] data;  
	protected SubscriptionImpl sub;

	/**
	 * 
	 */
	public Message() {
        this.subject = null;
        this.replyTo = null;
        this.data    = null;
        this.sub     = null;
	}
	
	/**
	 * @param subject the subject this {@code Message} will be published
	 * to, or that it was received from.
	 * @param reply the (optional) queue group name
	 * @param data the message payload
	 */
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
	
	/**
	 * Returns the message payload
	 * @return the message payload
	 */
	public byte[] getData() {
		return data;
	}
	
	/**
	 * Returns the message subject
	 * @return the message subject
	 */
	public String getSubject() {
		return subject;
	}
	
	/**
	 * Sets the subject of the message
	 * @param subject the subject to set
	 */
	public void setSubject(String subject) {
		this.subject = subject;
	}
	
	/**
	 * Returns the reply subject
	 * @return the reply subject
	 */
	public String getReplyTo() {
		return this.replyTo;
	}
	
	/**
	 * Sets the message reply subject
	 * @param replyTo the message reply subject
	 */
	public void setReplyTo(String replyTo) {
		this.replyTo = replyTo;
	}
	
	/**
	 * Returns the {@code Subscription} object the message 
	 * was received on
	 * @return the {@code Subscription} the message was received on
	 */
	public Subscription getSubscription() {
		return this.sub;
	}
	
	/**
	 * Sets the message payload data
	 * @param data the data
	 * @param offset the start offset in the data
	 * @param len the number of bytes to write
	 */
	public void setData(byte[] data, int offset, int len) {
		if (data == null) {
			this.data = null;
		} else {
			this.data = new byte[len];
			this.data = Arrays.copyOfRange(data, offset, len);
		}
	}

	/**
	 * Sets the message payload data
	 * @param data the data
	 */
	public void setData(byte[] data) {
		if (data == null) {
			this.data = null;
		} else {
			this.data = new byte[data.length];
			this.data = Arrays.copyOfRange(data, 0, data.length);
		}
	}

	/**
	 * @see java.lang.Object#toString()
	 * @return a string representation of the message
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

		int remainder = len - maxBytes;
		if (remainder > 0) {
            sb.append(String.format("%d more bytes", remainder));
		}
		
		sb.append(">}");

        return sb.toString();
    }
}
