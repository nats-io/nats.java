package io.nats.service;

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

    public static String toDiscoverySubject(String discoverySubject, String optionalServiceNameSegment, String optionalServiceIdSegment) {
        if (nullOrEmpty(optionalServiceIdSegment)) {
            if (nullOrEmpty(optionalServiceNameSegment)) {
                return DEFAULT_SERVICE_PREFIX + discoverySubject;
            }
            return DEFAULT_SERVICE_PREFIX + discoverySubject + "." + optionalServiceNameSegment;
        }
        return DEFAULT_SERVICE_PREFIX + discoverySubject + "." + optionalServiceNameSegment + "." + optionalServiceIdSegment;
    }
}
