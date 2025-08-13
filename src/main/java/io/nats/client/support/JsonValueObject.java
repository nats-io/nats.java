package io.nats.client.support;

import org.jspecify.annotations.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.nats.client.support.JsonUtils.addField;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;

/**
 * @see JsonValue
 * @see JsonValueUtils
 */
public class JsonValueObject extends JsonValue {
    public final List<String> mapOrder = new ArrayList<>();

    public JsonValueObject(@NonNull Map<String, JsonValue> map) {
        super(map);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, JsonValue> map() {
        return (Map<String, JsonValue>) value;
    }

    private static String valueString(Map<String, JsonValue> map, List<String> mapOrder) {
        StringBuilder sbo = beginJson();
        if (!mapOrder.isEmpty()) {
            for (String key : mapOrder) {
                addField(sbo, key, map.get(key));
            }
        }
        else {
            for (Map.Entry<String, JsonValue> entry : map.entrySet()) {
                addField(sbo, entry.getKey(), entry.getValue());
            }
        }
        return endJson(sbo).toString();
    }

    @Override
    public @NonNull JsonValueObject toJsonValue() {
        return this;
    }

    @Override
    public @NonNull String toJson() {
        return valueString(map(), mapOrder);
    }
}
