package io.nats.service;

import io.nats.client.support.Validator;

import java.time.Duration;

import static io.nats.client.support.Validator.nullOrEmpty;

public class ServiceUtil {

    public static final String PING = "PING";
    public static final String INFO = "INFO";
    public static final String SCHEMA = "SCHEMA";
    public static final String STATS = "STATS";
    public static final String DEFAULT_SERVICE_PREFIX = "$SRV.";
    public static final String QGROUP = "q";

    public static final Duration DEFAULT_DRAIN_TIMEOUT = Duration.ofSeconds(5);
    public static final long DEFAULT_DISCOVERY_MAX_TIME_MILLIS = 5000;
    public static final int DEFAULT_DISCOVERY_MAX_RESULTS = 10;

    public static String toDiscoverySubject(String discoveryName, String optionalServiceNameSegment, String optionalServiceIdSegment) {
        if (nullOrEmpty(optionalServiceIdSegment)) {
            if (nullOrEmpty(optionalServiceNameSegment)) {
                return DEFAULT_SERVICE_PREFIX + discoveryName;
            }
            return DEFAULT_SERVICE_PREFIX + discoveryName + "." + optionalServiceNameSegment;
        }
        return DEFAULT_SERVICE_PREFIX + discoveryName + "." + optionalServiceNameSegment + "." + optionalServiceIdSegment;
    }

    public static String validateEndpointName(String name) {
        return _validateEndpointSubject(name, "Endpoint Name");
    }

    public static String validateEndpointSubject(String name) {
        return _validateEndpointSubject(name, "Endpoint Subject");
    }

    public static String _validateEndpointSubject(String name, String label) {
        Validator.required(name, label);
        if (name != null) {
            // TODO ACTUAL VALIDATION
        }
        return name;
    }

    public static String validateGroupName(String name) {
        // TODO ACTUAL VALIDATION
        return name;
    }
}
