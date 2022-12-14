// Copyright 2022 The NATS Authors
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

package io.nats.service;

import java.time.Duration;

import static io.nats.client.support.Validator.nullOrEmpty;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public abstract class ServiceUtil {

    static final String PING = "PING";
    static final String INFO = "INFO";
    static final String SCHEMA = "SCHEMA";
    static final String STATS = "STATS";
    static final String DEFAULT_SERVICE_PREFIX = "$SRV.";
    static final String QGROUP = "q";

    public static final String NATS_SERVICE_ERROR = "Nats-Service-Error";
    public static final String NATS_SERVICE_ERROR_CODE = "Nats-Service-Error-Code";

    public static final Duration DEFAULT_DRAIN_TIMEOUT = Duration.ofSeconds(5);
    public static final long DEFAULT_DISCOVERY_MAX_TIME_MILLIS = 5000;
    public static final int DEFAULT_DISCOVERY_MAX_RESULTS = 10;

    private ServiceUtil() {} /* ensures cannot be constructed */

    static String toDiscoverySubject(String baseSubject, String optionalServiceNameSegment, String optionalServiceIdSegment) {
        if (nullOrEmpty(optionalServiceIdSegment)) {
            if (nullOrEmpty(optionalServiceNameSegment)) {
                return DEFAULT_SERVICE_PREFIX + baseSubject;
            }
            return DEFAULT_SERVICE_PREFIX + baseSubject + "." + optionalServiceNameSegment;
        }
        return DEFAULT_SERVICE_PREFIX + baseSubject + "." + optionalServiceNameSegment + "." + optionalServiceIdSegment;
    }
}
