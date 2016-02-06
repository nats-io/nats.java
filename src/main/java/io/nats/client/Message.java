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

import java.nio.ByteBuffer;
import java.util.Arrays;

import io.nats.client.Parser.MsgArg;

/**
 * A {@code Message} object is used to send a message containing a 
 * stream of uninterpreted bytes.
 */
public class Message {
	private byte[] subject;
	private String subjectString;
	private byte[] replyTo;
	private String replyToString;
	private byte[] data;  
	protected SubscriptionImpl sub;

	/**
	 * 
	 */
	public Message() {
//        this.subject = null;
//        this.replyTo = null;
//        this.data    = null;
//        this.sub     = null;
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
	
//	protected Message(MsgArg ma, SubscriptionImpl sub, byte[] data, int length) {
//		this(data, length, ma.subject, ma.reply, sub);        
//	}

	protected Message(MsgArg ma, SubscriptionImpl sub, byte[] buf, int offset, int length) {
		this.setSubject(ma.subject, ma.subjectLength);
		this.setReplyTo(ma.reply, ma.replyLength);
		this.sub = sub;		
		// make a deep copy of the bytes for this message.
		if (ma.size > 0) {
			this.data = new byte[ma.size];
			try {
			System.arraycopy(buf, offset, this.data, 0, length);
			} catch (ArrayIndexOutOfBoundsException e) {
				System.err.printf("buf.length=%d, offset=%d, data.length=%d, length=%d\n, buf=[%s]",
						buf.length, offset, data.length, length, new String(buf));
				e.printStackTrace();				
			}
		}
	}

//	protected Message(byte[] data, int length, byte[] subject, byte[] reply, SubscriptionImpl sub)
//	{
//		this.setSubject(subject);
//        // make a deep copy of the bytes for this message.
//        this.data = Arrays.copyOf(data, length);
//        this.setReplyTo(reply);
//        this.sub = sub;		
//	}
	
	protected Message(byte[] data, int length, String subject, String reply, SubscriptionImpl sub)
	{
		this.setSubject(subject);
        // make a deep copy of the bytes for this message.
		if (data != null) {
			this.data = Arrays.copyOf(data, length);
		}
        this.setReplyTo(reply);
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
		if (subjectString == null) {
			subjectString = new String(subject);
			return subjectString;
		}
		return subjectString;
	}

	byte[] getSubjectBytes() {
		return subject;
	}

	/**
	 * Sets the subject of the message
	 * @param subject the subject to set
	 */
	public void setSubject(String subject) {
		String s = subject.trim();
        if (s==null || s.isEmpty())
        {
            throw new IllegalArgumentException(
                "Subject cannot be null, empty, or whitespace.");
        }
		this.subject = s.getBytes();
	}
	
	void setSubject(byte[] subject, int length) {
		this.subject = Arrays.copyOf(subject, length);
	}
	
	/**
	 * Returns the reply subject
	 * @return the reply subject
	 */
	public String getReplyTo() {
		if (replyToString == null) {
			if (replyTo != null) {
				replyToString = new String(replyTo);
			}
		}
		return replyToString;
	}

	byte[] getReplyToBytes() {
		return replyTo;
	}

	/**
	 * Sets the message reply subject
	 * @param replyTo the message reply subject
	 */
	public void setReplyTo(String replyTo) {
		if (replyTo == null) {
			this.replyTo=null;
		}
		if (replyTo != null) {
			String r = replyTo.trim();
			if (r.isEmpty()) {
				throw new IllegalArgumentException(
						"Reply subject cannot be empty or whitespace.");
			}
			this.replyTo = replyTo.getBytes();
		}
	}

	void setReplyTo(byte[] replyTo, int length) {
		if (replyTo==null) {
			this.replyTo = null;
		} else {
			this.replyTo = Arrays.copyOf(replyTo, length);
		}
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
