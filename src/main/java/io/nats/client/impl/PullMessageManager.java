// Copyright 2021 The NATS Authors
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

package io.nats.client.impl;

import io.nats.client.JetStreamStatusException;
import io.nats.client.Message;

import java.util.Arrays;
import java.util.List;

class PullMessageManager extends MessageManager {
    /*
        Known Pull Statuses
        -------------------------------------------
        409 Consumer is push based
        409 Exceeded MaxRequestBatch of %d
        409 Exceeded MaxRequestExpires of %v
        409 Exceeded MaxRequestMaxBytes of %v
        409 Message Size Exceeds MaxBytes
        409 Exceeded MaxWaiting
        404 No Messages
        408 Request Timeout
    */

    private static final List<Integer> MANAGED_STATUS_CODES = Arrays.asList(404, 408);

    boolean manage(Message msg) {
        if (msg.isStatusMessage()) {
            if ( MANAGED_STATUS_CODES.contains(msg.getStatus().getCode()) ) {
                return true;
            }
            throw new JetStreamStatusException(sub, msg.getStatus());
        }
        return false;
    }
}
