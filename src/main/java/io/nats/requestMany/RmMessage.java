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

/**
 * This class is EXPERIMENTAL, meaning it's api is subject to change.
 */
public class RmMessage {
    private static final AtomicLong ID_MAKER = new AtomicLong();

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

    /**
     * The internal id of the message, in case the user wants to track.
     * This number is only unique to when this class was statically initialized.
     * @return the id.
     */
    public long getId() {
        return id;
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
        if (isEndOfData()) {
            if (isNormalEndOfData()) {
                return "RMM EOD, Normal";
            }
            if (isStatusMessage()){
                return "RMM EOD, Abnormal: " + message;
            }
            return "RMM EOD, Abnormal: " + exception;
        }
        if (message.getData() == null || message.getData().length == 0) {
            return "RMM Data Message: Empty Payload";
        }
        return "RMM Data Message: " + new String(message.getData());
    }
}
