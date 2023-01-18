package io.nats.service;

import io.nats.client.support.Validator;

import java.time.Duration;

import static io.nats.client.support.Validator.nullOrEmpty;
import static io.nats.client.support.Validator.validateSubject;

public abstract class ServiceUtil {

    public static final String SRV_PING = "PING";
    public static final String SRV_INFO = "INFO";
    public static final String SRV_SCHEMA = "SCHEMA";
    public static final String SRV_STATS = "STATS";
    public static final String DEFAULT_SERVICE_PREFIX = "$SRV.";
    public static final String QGROUP = "q";

    public static final Duration DEFAULT_DRAIN_TIMEOUT = Duration.ofSeconds(5);
    public static final long DEFAULT_DISCOVERY_MAX_TIME_MILLIS = 5000;
    public static final int DEFAULT_DISCOVERY_MAX_RESULTS = 10;

    private ServiceUtil() {} /* ensures cannot be constructed */

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
        return Validator.validateIsRestrictedTerm(name, "Endpoint Name", true);
    }

    public static String validateEndpointSubject(String subject, String dflt) {
        String valid = validateSubject(subject, "Endpoint Subject", false, false);
        return valid == null ? dflt : valid;
    }

    public static String validateGroupName(String name) {
        return validateSubject(name, "Group Name", true, true);
    }
}
