package io.nats.client.impl;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.*;

class NatsEncoder {
    private static final ThreadLocal<Charset> protocolCharset = new ThreadLocal<>();
    private static final ThreadLocal<CharsetEncoder> protocolEncoderCache = new ThreadLocal<>();

    private static final ThreadLocal<Charset> subjectCharset = new ThreadLocal<>();
    private static final ThreadLocal<CharsetDecoder> subjectDecoderCache = new ThreadLocal<>();
    private static final ThreadLocal<CharsetEncoder> subjectEncoderCache = new ThreadLocal<>();

    private static final ThreadLocal<Charset> replyToCharset = new ThreadLocal<>();
    private static final ThreadLocal<CharsetDecoder> replyToDecoderCache = new ThreadLocal<>();
    private static final ThreadLocal<CharsetEncoder> replyToEncoderCache = new ThreadLocal<>();

    private static final ThreadLocal<Charset> queueCharset = new ThreadLocal<>();
    private static final ThreadLocal<CharsetDecoder> queueDecoderCache = new ThreadLocal<>();
    private static final ThreadLocal<CharsetEncoder> queueEncoderCache = new ThreadLocal<>();

    private static final ThreadLocal<Charset> inboxCharset = new ThreadLocal<>();
    private static final ThreadLocal<CharsetDecoder> inboxDecoderCache = new ThreadLocal<>();
    private static final ThreadLocal<CharsetEncoder> inboxEncoderCache = new ThreadLocal<>();

    private static final ThreadLocal<Charset> sidCharset = new ThreadLocal<>();
    private static final ThreadLocal<CharsetDecoder> sidDecoderCache = new ThreadLocal<>();
    private static final ThreadLocal<CharsetEncoder> sidEncoderCache = new ThreadLocal<>();

    private static Charset getProtocolCharset() {
        Charset c = protocolCharset.get();
        if (c == null) {
            c = StandardCharsets.UTF_8;
            protocolCharset.set(StandardCharsets.UTF_8);
        }
        return c;
    }

    private static CharsetEncoder getProtocolEncoder() {
        CharsetEncoder encoder = protocolEncoderCache.get();
        if (encoder == null) {
            encoder = getProtocolCharset().newEncoder();
            protocolEncoderCache.set(encoder);
        }
        return encoder;
    }

    static CoderResult encodeProtocol(CharBuffer in, ByteBuffer out, boolean endOfInput) {
        return getProtocolEncoder().encode(in, out, endOfInput);
    }

    private static Charset getSubjectCharset() {
        Charset c = subjectCharset.get();
        if (c == null) {
            c = StandardCharsets.UTF_8;
            subjectCharset.set(c);
        }
        return c;
    }

    private static CharsetEncoder getSubjectEncoder() {
        CharsetEncoder encoder = subjectEncoderCache.get();
        if (encoder == null) {
            encoder = getSubjectCharset().newEncoder();
            subjectEncoderCache.set(encoder);
        }
        return encoder;
    }

    static ByteBuffer encodeSubject(String in) {
        try {
            return getSubjectEncoder().encode(CharBuffer.wrap(in));
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static CharsetDecoder getSubjectDecoder() {
        CharsetDecoder decoder = subjectDecoderCache.get();
        if (decoder == null) {
            decoder = getSubjectCharset().newDecoder();
            subjectDecoderCache.set(decoder);
        }
        return decoder;
    }

    static String decodeSubject(ByteBuffer in) {
        try {
            return getSubjectDecoder().decode(in).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static Charset getReplyToCharset() {
        Charset c = replyToCharset.get();
        if (c == null) {
            c = StandardCharsets.UTF_8;
            replyToCharset.set(c);
        }
        return c;
    }

    private static CharsetEncoder getReplyToEncoder() {
        CharsetEncoder encoder = replyToEncoderCache.get();
        if (encoder == null) {
            encoder = getReplyToCharset().newEncoder();
            replyToEncoderCache.set(encoder);
        }
        return encoder;
    }

    static ByteBuffer encodeReplyTo(String in) {
        try {
            return getReplyToEncoder().encode(CharBuffer.wrap(in));
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static CharsetDecoder getReplyToDecoder() {
        CharsetDecoder decoder = replyToDecoderCache.get();
        if (decoder == null) {
            decoder = getReplyToCharset().newDecoder();
            replyToDecoderCache.set(decoder);
        }
        return decoder;
    }

    static String decodeReplyTo(ByteBuffer in) {
        try {
            return getReplyToDecoder().decode(in).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static Charset getQueueCharset() {
        Charset c = queueCharset.get();
        if (c == null) {
            c = StandardCharsets.UTF_8;
            queueCharset.set(c);
        }
        return c;
    }

    private static CharsetEncoder getQueueEncoder() {
        CharsetEncoder encoder = queueEncoderCache.get();
        if (encoder == null) {
            encoder = getQueueCharset().newEncoder();
            queueEncoderCache.set(encoder);
        }
        return encoder;
    }

    static ByteBuffer encodeQueue(String in) {
        try {
            return getQueueEncoder().encode(CharBuffer.wrap(in));
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static CharsetDecoder getQueueDecoder() {
        CharsetDecoder decoder = queueDecoderCache.get();
        if (decoder == null) {
            decoder = getQueueCharset().newDecoder();
            queueDecoderCache.set(decoder);
        }
        return decoder;
    }

    static String decodeQueue(ByteBuffer in) {
        try {
            return getQueueDecoder().decode(in).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static Charset getInboxCharset() {
        Charset c = inboxCharset.get();
        if (c == null) {
            c = StandardCharsets.UTF_8;
            inboxCharset.set(c);
        }
        return c;
    }

    private static CharsetEncoder getInboxEncoder() {
        CharsetEncoder encoder = inboxEncoderCache.get();
        if (encoder == null) {
            encoder = getInboxCharset().newEncoder();
            inboxEncoderCache.set(encoder);
        }
        return encoder;
    }

    static ByteBuffer encodeInbox(String in) {
        try {
            return getInboxEncoder().encode(CharBuffer.wrap(in));
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static CharsetDecoder getInboxDecoder() {
        CharsetDecoder decoder = inboxDecoderCache.get();
        if (decoder == null) {
            decoder = getInboxCharset().newDecoder();
            inboxDecoderCache.set(decoder);
        }
        return decoder;
    }

    static String decodeInbox(ByteBuffer in) {
        try {
            return getInboxDecoder().decode(in).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static Charset getSIDCharset() {
        Charset c = sidCharset.get();
        if (c == null) {
            c = StandardCharsets.US_ASCII;
            sidCharset.set(c);
        }
        return c;
    }

    private static CharsetEncoder getSIDEncoder() {
        CharsetEncoder encoder = sidEncoderCache.get();
        if (encoder == null) {
            encoder = getSIDCharset().newEncoder();
            sidEncoderCache.set(encoder);
        }
        return encoder;
    }

    static ByteBuffer encodeSID(String in) {
        try {
            return getSIDEncoder().encode(CharBuffer.wrap(in));
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static CharsetDecoder getSIDDecoder() {
        CharsetDecoder decoder = sidDecoderCache.get();
        if (decoder == null) {
            decoder = getSIDCharset().newDecoder();
            sidDecoderCache.set(decoder);
        }
        return decoder;
    }

    static String decodeSID(ByteBuffer in) {
        try {
            return getSIDDecoder().decode(in).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
