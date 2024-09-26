// Copyright 2024 The NATS Authors
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

package io.nats.RequestMany;

import io.nats.client.Message;
import io.nats.client.impl.StatusMessage;

public class RequestManyMessage {
    public static final RequestManyMessage NORMAL_EOD = new RequestManyMessage((Exception)null);

    private final Message message;
    private final Exception exception;
    private final boolean isRegularMessage;
    private final boolean isStatusMessage;

    RequestManyMessage(Message m) {
        message = m;
        exception = null;
        isStatusMessage = m != null && m.isStatusMessage();
        isRegularMessage = m != null && !isStatusMessage;
    }

    RequestManyMessage(Exception e) {
        message = null;
        exception = e;
        isStatusMessage = false;
        isRegularMessage = false;
    }

    public Message getMessage() {
        return message;
    }

    public StatusMessage getStatusMessage() {
        return isStatusMessage ? (StatusMessage)message : null;
    }

    public Exception getException() {
        return exception;
    }

    public boolean isRegularMessage() {
        return isRegularMessage;
    }

    public boolean isStatusMessage() {
        return isStatusMessage;
    }

    public boolean isException() {
        return exception != null;
    }

    public boolean isEndOfData() {
        return message == null || isStatusMessage || exception != null;
    }

    public boolean isNormalEndOfData() {
        return message == null && exception == null;
    }

    public boolean isAbnormalEndOfData() {
        return isStatusMessage || exception != null;
    }
}
