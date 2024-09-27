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

package io.nats.requestMany;

import io.nats.client.Message;
import io.nats.client.impl.StatusMessage;

import java.util.concurrent.atomic.AtomicLong;

public class RmMessage {
    public static AtomicLong ID_MAKER = new AtomicLong();

    public static final RmMessage NORMAL_EOD = new RmMessage();

    private final Message message;
    private final Exception exception;
    private final long id;

    RmMessage(Message m) {
        message = m;
        exception = null;
        id = ID_MAKER.incrementAndGet();
    }

    RmMessage(Exception e) {
        message = null;
        exception = e;
        id = ID_MAKER.incrementAndGet();
    }

    private RmMessage() {
        message = null;
        exception = null;
        id = 0;
    }

    public Message getMessage() {
        return message;
    }

    public StatusMessage getStatusMessage() {
        return isStatusMessage() ? (StatusMessage)message : null;
    }

    public Exception getException() {
        return exception;
    }

    public boolean isDataMessage() {
        return message != null && !isStatusMessage();
    }

    public boolean isStatusMessage() {
        return message != null && message.isStatusMessage();
    }

    public boolean isException() {
        return exception != null;
    }

    public boolean isEndOfData() {
        return message == null || isStatusMessage() || exception != null;
    }

    public boolean isNormalEndOfData() {
        return message == null && exception == null;
    }

    public boolean isAbnormalEndOfData() {
        return isStatusMessage() || exception != null;
    }

    @Override
    public String toString() {
        return "Rmm{id=" + id +
            ", message=" + message +
            ", exception=" + exception +
            '}';
    }
}
