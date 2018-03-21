// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.client;

import io.nats.client.Parser.MsgArg;
import java.util.Arrays;

/**
 * A {@code Message} object is used to send a message containing a stream of uninterpreted bytes.
 */
public class Message {
    private SubscriptionImpl sub;
    private byte[] subjectBytes;
    private String subjectString;
    private byte[] replyToBytes;
    private String replyToString;
    private byte[] data;

    /**
     * Message constructor.
     */
    public Message() {
    }

    /**
     * @param subject the subject this {@code Message} will be published to, or that it was received
     *                from.
     * @param reply   the (optional) reply subject name
     * @param data    the message payload
     */
    public Message(String subject, String reply, byte[] data) {
        this(data, subject, reply, null);
    }

    /*
     * Note that this constructor may throw ArrayIndexOutOfBoundsException
     */
    Message(MsgArg ma, SubscriptionImpl sub, byte[] buf, int offset, int length) {
        this.setSubject(ma.subject.array(), ma.subject.limit());
        if (ma.reply.limit() > 0) {
            this.setReplyTo(ma.reply.array(), ma.reply.limit());
        }
        this.sub = sub;
        // make a deep copy of the bytes for this message.
        if (length > 0) {
            if (length > buf.length) {
                String msg = String.format(
                        "nats: source buffer size (%d) smaller than requested copy length(%d)",
                        length, buf.length);
                throw new IllegalArgumentException(msg);
            } else if (length > ma.size) {
                String msg = String.format(
                        "nats: requested copy length(%d) larger than ma.size(%d)",
                        length, ma.size);
                throw new IllegalArgumentException(msg);
            }
            data = new byte[length];
            System.arraycopy(buf, offset, data, 0, length);
        }
    }

    Message(byte[] data, String subject, String reply, SubscriptionImpl sub) {
        if (subject == null) {
            throw new NullPointerException("Subject cannot be null");
        }
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

    /**
     * Returns the message subject.
     *
     * @return the message subject
     */
    public String getSubject() {
        if (subjectString == null && subjectBytes != null) {
            subjectString = new String(subjectBytes, 0, subjectBytes.length);
        }
        return subjectString;
    }

    /**
     * Sets the subject of the message.
     *
     * @param subject the subject to set
     */
    public void setSubject(final String subject) {
        if (subject == null || subject.trim().isEmpty()) {
            throw new IllegalArgumentException("Subject cannot be null, empty, or whitespace.");
        }
        this.subjectString = subject.trim();
        this.subjectBytes = subjectString.getBytes();
    }

    byte[] getSubjectBytes() {
        return subjectBytes;
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
        if (replyToString == null && replyToBytes != null) {
            replyToString = new String(replyToBytes, 0, replyToBytes.length);
        }
        return replyToString;
    }

    /**
     * Sets the message reply subject.
     *
     * @param replyTo the message reply subject
     */
    public void setReplyTo(String replyTo) {
        if (replyTo == null) {
            this.replyToBytes = null;
            this.replyToString = null;
        } else {
            String reply = replyTo.trim();
            if (reply.isEmpty()) {
                throw new IllegalArgumentException("Reply subject cannot be empty or whitespace.");
            }
            this.replyToString = replyTo;
            this.replyToBytes = replyTo.getBytes();
        }
    }

    byte[] getReplyToBytes() {
        return replyToBytes;
    }

    void setReplyTo(byte[] replyTo, int length) {
        if (replyTo == null) {
            this.replyToBytes = null;
            this.replyToString = null;
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
     * @param data   the data
     * @param offset the start offset in the data
     * @param length the number of bytes to write
     */
    public void setData(byte[] data, int offset, int length) {
        if (data == null) {
            this.data = null;
        } else {
            if (length > data.length) {
                throw new IllegalArgumentException(
                        "nats: source buffer smaller than requested copy length");
            }
            this.data = new byte[length];
            System.arraycopy(data, offset, this.data, 0, length);
        }
    }

    /**
     * @return a string representation of the message
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        final int maxBytes = 32;
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
