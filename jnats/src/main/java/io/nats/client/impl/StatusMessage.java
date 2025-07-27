// Copyright 2015-2022 The NATS Authors
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

import io.nats.client.support.Status;

public class StatusMessage extends IncomingMessage {
    private final Status status;

    StatusMessage(Status status) {
        this.status = status;
    }

    @Override
    public boolean isStatusMessage() {
        return true;
    }

    @Override
    public Status getStatus() {
        return status;
    }

    @Override
    public String toString() {
        return "StatusMessage{" +
            "code=" + status.getCode() +
            ", message='" + status.getMessage() + '\'' +
            '}';
    }
}
