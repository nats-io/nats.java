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

import io.nats.service.api.EndpointStats;

import java.time.Duration;
import java.util.Comparator;

import static io.nats.client.support.Validator.nullOrEmpty;

public abstract class ServiceUtil {

    static final String PING = "PING";
    static final String INFO = "INFO";
    static final String SCHEMA = "SCHEMA";
    static final String STATS = "STATS";
    static final String SRV_PREFIX = "$SRV.";

    static final String QGROUP = "q";

    public static final Duration DEFAULT_DRAIN_TIMEOUT = Duration.ofSeconds(5);
    public static final long DEFAULT_DISCOVERY_MAX_TIME_MILLIS = 5000;
    public static final int DEFAULT_DISCOVERY_MAX_RESULTS = 10;

    private ServiceUtil() {} /* ensures cannot be constructed */

    static String toDiscoverySubject(String name) {
        return toDiscoverySubject(name, null, null);
    }

    static String toDiscoverySubject(String name, String serviceName) {
        return toDiscoverySubject(name, serviceName, null);
    }

    static String toDiscoverySubject(String name, String serviceName, String serviceId) {
        if (nullOrEmpty(serviceId)) {
            if (nullOrEmpty(serviceName)) {
                return SRV_PREFIX + name;
            }
            return SRV_PREFIX + name + "." + serviceName;
        }
        return SRV_PREFIX + name + "." + serviceName + "." + serviceId;
    }

    static Comparator<EndpointStats> ENDPOINT_STATS_COMPARATOR = new Comparator<EndpointStats>() {
        @Override
        public int compare(EndpointStats o1, EndpointStats o2) {
            return order(o1).compareTo(order(o2));
        }

        private Integer order(EndpointStats es) {
            switch (es.name) {
                case PING: return 1;
                case INFO: return 2;
                case SCHEMA: return 3;
                case STATS: return 4;
            }
            return 0;
        }
    };
}
