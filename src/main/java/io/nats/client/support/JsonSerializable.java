package io.nats.client.support;

import java.nio.charset.StandardCharsets;

public interface JsonSerializable {
    String toJson();

    default byte[] serialize() {
        return toJson().getBytes(StandardCharsets.UTF_8);
    }
}
