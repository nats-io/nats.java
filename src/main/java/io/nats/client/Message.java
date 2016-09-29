/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/
/**
 * 
 */

package io.nats.client;

import io.nats.client.Parser.MsgArg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * A {@code Message} object is used to send a message containing a stream of uninterpreted bytes.
 */
public class Message {
    static final Logger logger = LoggerFactory.getLogger(Message.class);
    private byte[] subjectBytes;
    private String subjectString;
    // private ByteBuffer subject;
    // private ByteBuffer replyTo;
    private byte[] replyToBytes;
    private String replyToString;
    // private ByteBuffer data;
    private byte[] data;
    protected SubscriptionImpl sub;

    /**
     * Message constructor.
     */
    public Message() {}

    /**
     * @param subject the subject this {@code Message} will be published to, or that it was received
     *        from.
     * @param reply the (optional) queue group name
     * @param data the message payload
     */
    public Message(String subject, String reply, byte[] data) {
        this(data, (null != data ? data.length : 0), subject, reply, null);
    }

    // protected Message(MsgArg ma, SubscriptionImpl sub, byte[] data, int length) {
    // this(data, length, ma.subject, ma.reply, sub);
    // }

    protected Message(MsgArg ma, SubscriptionImpl sub, byte[] buf, int offset, int length) {
        this.setSubject(ma.subject.array(), ma.subject.limit());
        if (ma.reply.limit() > 0) {
            this.setReplyTo(ma.reply.array(), ma.reply.limit());
        }
        this.sub = sub;
        // make a deep copy of the bytes for this message.
        if (ma.size > 0) {
            data = new byte[ma.size];
            try {
                sysArrayCopy(buf, offset, data, 0, length);
            } catch (ArrayIndexOutOfBoundsException e) {
                logger.debug("nats: unexpected runtime error copying message body");
                logger.debug("buf.length={}, offset={}, data.length={}, length={}\n, buf=[{}]",
                        buf.length, offset, data.length, length, new String(buf, offset, length));
                e.printStackTrace();
            }
        }
    }

    protected Message(byte[] data, int length, String subject, String reply, SubscriptionImpl sub) {
        this.setSubject(subject);
        // make a deep copy of the bytes for this message.
        this.setData(data);
        this.setReplyTo(reply);
        this.sub = sub;
    }

    /**
     * Returns the message payload.
     * 
     * @return the message payload
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Returns the message subject.
     * 
     * @return the message subject
     */
    public String getSubject() {
        if (subjectString == null) {
            subjectString = new String(subjectBytes, 0, subjectBytes.length);
        }
        return subjectString;
    }

    byte[] getSubjectBytes() {
        return subjectBytes;
    }

    /**
     * Sets the subject of the message.
     * 
     * @param subject the subject to set
     */
    public void setSubject(final String subject) {
        String subj = subject.trim();
        if (subj == null || subj.isEmpty()) {
            throw new IllegalArgumentException("Subject cannot be null, empty, or whitespace.");
        }
        this.subjectString = subj;
        this.subjectBytes = subj.getBytes();
    }

    void setSubject(byte[] subject, int length) {
        this.subjectBytes = Arrays.copyOf(subject, length);
    }

    /**
     * Returns the reply subject.
     * 
     * @return the reply subject
     */
    public String getReplyTo() {
        if (replyToString == null) {
            if (replyToBytes != null) {
                replyToString = new String(replyToBytes, 0, replyToBytes.length);
            }
        }
        return replyToString;
    }

    byte[] getReplyToBytes() {
        return replyToBytes;
    }

    /**
     * Sets the message reply subject.
     * 
     * @param replyTo the message reply subject
     */
    public void setReplyTo(String replyTo) {
        if (replyTo == null) {
            this.replyToBytes = null;
        } else {
            String reply = replyTo.trim();
            if (reply.isEmpty()) {
                throw new IllegalArgumentException("Reply subject cannot be empty or whitespace.");
            }
            this.replyToString = replyTo;
            this.replyToBytes = replyTo.getBytes();
        }
    }

    void setReplyTo(byte[] replyTo, int length) {
        if (replyTo == null) {
            this.replyToBytes = null;
        } else {
            this.replyToBytes = Arrays.copyOf(replyTo, length);
        }
    }

    /**
     * Returns the {@code Subscription} object the message was received on.
     * 
     * @return the {@code Subscription} the message was received on
     */
    public Subscription getSubscription() {
        return this.sub;
    }

    /**
     * Sets the message payload data.
     * 
     * @param data the data
     * @param offset the start offset in the data
     * @param length the number of bytes to write
     */
    public void setData(byte[] data, int offset, int length) {
        if (data == null) {
            this.data = null;
        } else {
            this.data = new byte[length];
            sysArrayCopy(data, offset, this.data, 0, length);
        }
    }

    /**
     * Sets the message payload data.
     * 
     * @param data the data
     */
    public void setData(byte[] data) {
        if (data != null) {
            setData(data, 0, data.length);
        } else {
            setData(null, 0, 0);
        }
    }

    static void sysArrayCopy(byte[] src, int srcPos, byte[] dest, int destPos, int length) {
        if (length > src.length) {
            throw new ArrayIndexOutOfBoundsException(
                    "source buffer smaller than requested copy length");
        } else if (length > dest.length) {
            throw new ArrayIndexOutOfBoundsException(
                    "destination buffer smaller than requested copy length");
        }
        System.arraycopy(src, srcPos, dest, destPos, length);
    }

    /**
     * @see java.lang.Object#toString()
     * @return a string representation of the message
     */
    @Override
    public String toString() {
        int maxBytes = 32;
        int len = 0;

        byte[] buf = getData();
        if (buf != null) {
            len = buf.length;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("{Subject=%s;Reply=%s;Payload=<", getSubject(), getReplyTo()));

        for (int i = 0; i < maxBytes && i < len; i++) {
            sb.append((char) buf[i]);
        }

        int remainder = len - maxBytes;
        if (remainder > 0) {
            sb.append(String.format("%d more bytes", remainder));
        }

        sb.append(">}");

        return sb.toString();
    }
}
