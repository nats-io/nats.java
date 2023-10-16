package io.nats.compatibility;

import io.nats.client.Connection;
import io.nats.client.support.JsonParseException;
import io.nats.client.support.JsonParser;
import io.nats.client.support.JsonValue;
import io.nats.client.support.JsonValueUtils;

public abstract class Command extends TestMessage {
    public final Connection nc;

    // info from the message data
    public final JsonValue full;
    public final JsonValue config;

    protected Command(Connection nc, TestMessage tm) {
        super(tm);
        this.nc = nc;

        JsonValue tempDataValue = null;
        JsonValue tempConfig = null;
        if (payload != null && payload.length > 0) {
            try {
                tempDataValue = JsonParser.parse(payload);
                Log.info("CMD", subject, tempDataValue .toJson());
                tempConfig = JsonValueUtils.readObject(tempDataValue, "config");
            }
            catch (JsonParseException e) {
                handleException(e);
            }
        }

        full = tempDataValue;
        config = tempConfig;
    }

    protected void respond() {
        Log.info("RESPOND " + subject);
        nc.publish(replyTo, null);
    }

    protected void respond(String payload) {
        Log.info("RESPOND " + subject + " with " + payload);
        nc.publish(replyTo, payload.getBytes());
    }

    @Override
    public String toString() {
        return full.toJson();
    }

    protected void handleException(Exception e) {
        Log.error(subject, e);
    }
}
