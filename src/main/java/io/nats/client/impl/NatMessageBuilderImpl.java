package io.nats.client.impl;

import io.nats.client.Message;
import io.nats.client.MessageBuilder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class NatMessageBuilderImpl implements MessageBuilder {

    private String subject;
    private String replyTo;
    private byte[] data;
    private boolean utf8mode;
    private boolean hpub;
    private Map<String, List<String>> headers;

    @Override
    public String getSubject() {
        return subject;
    }

    @Override
    public MessageBuilder withSubject(String subject) {
        this.subject = subject;
        return this;
    }

    @Override
    public String getReplyTo() {
        return replyTo;
    }

    @Override
    public MessageBuilder withReplyTo(String replyTo) {
        this.replyTo = replyTo;
        return this;
    }

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public MessageBuilder withData(byte[] data) {
        this.data = data;
        return this;
    }

    @Override
    public boolean isUtf8mode() {
        return utf8mode;
    }

    @Override
    public MessageBuilder withUtf8mode(boolean utf8mode) {
        this.utf8mode = utf8mode;
        return this;
    }

    @Override
    public boolean isHpub() {
        return hpub;
    }

    @Override
    public MessageBuilder withHpub(boolean hpub) {
        this.hpub = hpub;
        return this;
    }

    @Override
    public Map<String, List<String>> getHeaders() {
        if (headers == null) {
            headers = new LinkedHashMap<>();
        }
        return headers;
    }

    @Override
    public MessageBuilder withHeaders(LinkedHashMap<String, List<String>> headers) {
        this.headers = headers;
        if (headers != null)
            this.hpub = true;
        return this;
    }

    @Override
    public MessageBuilder withHeaders(Map<String, List<String>> headers) {
        if (headers instanceof LinkedHashMap) {
            this.headers = headers;
            this.hpub = true;
        } else {
            if (headers!=null)
            this.getHeaders().putAll(headers);
        }
        return this;
    }

    @Override
    public MessageBuilder addHeader(String headerName, String headerValue) {
        Map<String, List<String>> headers = this.getHeaders();
        List<String> list = headers.computeIfAbsent(headerName, k -> new ArrayList<>());
        list.add(headerValue);
        this.hpub = true;
        return this;
    }

    @Override
    public Message build() {
        if (headers!=null)
            return new NatsMessage(subject, replyTo, data, utf8mode, hpub, (LinkedHashMap<String, List<String>>) headers);
        else
            return new NatsMessage(subject, replyTo, data, utf8mode, hpub, null);

    }
}
