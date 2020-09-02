package io.nats.client;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface MessageBuilder {
    String getSubject();

    MessageBuilder withSubject(String subject);

    String getReplyTo();

    MessageBuilder withReplyTo(String replyTo);

    byte[] getData();

    MessageBuilder withData(byte[] data);

    boolean isUtf8mode();

    MessageBuilder withUtf8mode(boolean utf8mode);

    boolean isHpub();

    MessageBuilder withHpub(boolean hpub);

    Map<String, List<String>> getHeaders();

    MessageBuilder withHeaders(LinkedHashMap<String, List<String>> headers);

    MessageBuilder withHeaders(Map<String, List<String>> headers);

    MessageBuilder addHeader(String headerName, String headerValue);

    Message build();

}
