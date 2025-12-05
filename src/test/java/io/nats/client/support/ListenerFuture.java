// Copyright 2025 The NATS Authors
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

package io.nats.client.support;

import io.nats.client.ConnectionListener;
import io.nats.client.ErrorListener;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

// ----------------------------------------------------------------------------------------------------
// Prep
// ----------------------------------------------------------------------------------------------------
public class ListenerFuture extends CompletableFuture<Void> {
    public ConnectionListener.Events eventType;
    public String error;
    public Class<?> exceptionClass;
    public String exContains;
    public ListenerStatusType lbfStatusType;
    public int statusCode = -1;
    public String fcSubject;
    public ErrorListener.FlowControlSource fcSource;
    public boolean forHeartbeat;
    public boolean forSocketWriteTimeout;

    public Throwable receivedException;

    public int validateTimeout;

    ListenerFuture(ConnectionListener.Events type, int validateTimeout) {
        this.eventType = type;
        this.validateTimeout = validateTimeout;
    }

    ListenerFuture(Class<?> exceptionClass, int validateTimeout) {
        this.exceptionClass = exceptionClass;
        this.validateTimeout = validateTimeout;
    }

    ListenerFuture(Class<?> exceptionClass, String contains, int validateTimeout) {
        this.exceptionClass = exceptionClass;
        this.exContains = contains;
        this.validateTimeout = validateTimeout;
    }

    ListenerFuture(String errorText, int validateTimeout) {
        error = errorText;
        this.validateTimeout = validateTimeout;
    }

    ListenerFuture(ListenerStatusType type, int statusCode, int validateTimeout) {
        lbfStatusType = type;
        this.statusCode = statusCode;
        this.validateTimeout = validateTimeout;
    }

    ListenerFuture(String fcSubject, ErrorListener.FlowControlSource fcSource, int validateTimeout) {
        this.fcSubject = fcSubject;
        this.fcSource = fcSource;
        this.validateTimeout = validateTimeout;
    }

    public ListenerFuture(boolean forHeartbeat, boolean forSocketWriteTimeout, int validateTimeout) {
        this.forHeartbeat = forHeartbeat;
        this.forSocketWriteTimeout = forSocketWriteTimeout;
        this.validateTimeout = validateTimeout;
    }

    @Override
    public String toString() {
        return "ListenerFuture{" + getDetails() + "}";
    }

    public List<String> getDetails() {
        List<String> details = new ArrayList<>();
        if (eventType != null) {
            details.add(eventType.toString());
        }
        if (error != null) {
            details.add(error);
        }
        if (exceptionClass != null) {
            details.add(exceptionClass.toString());
        }
        if (lbfStatusType != null) {
            details.add(lbfStatusType.toString());
        }
        if (statusCode != -1) {
            details.add(Integer.toString(statusCode));
        }
        if (fcSubject != null) {
            details.add(fcSubject);
        }
        if (fcSource != null) {
            details.add(fcSource.toString());
        }
        return details;
    }
}
