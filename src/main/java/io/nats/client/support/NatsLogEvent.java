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

package io.nats.client.support;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Supplier;
import java.util.logging.Level;

public class NatsLogEvent {
    private final Level logLevel;

    private final String className;

    private final String message;

    private final Supplier<String> msgSupplier;

    private final Throwable throwable;

    private final ZonedDateTime eventTime = ZonedDateTime.now();

    public NatsLogEvent(final Level logLevel, final String className, final String message) {
        this(logLevel, className, message, null, null);
    }

    public NatsLogEvent(final Level logLevel, final String className, final String message, final Throwable throwable) {
        this(logLevel, className, message, null, throwable);
    }

    public NatsLogEvent(final Level logLevel, final String className, final Supplier<String> msgSupplier) {
        this(logLevel, className, null, msgSupplier, null);
    }

    public NatsLogEvent(final Level logLevel, final String className, final Supplier<String> msgSupplier, Throwable throwable) {
        this(logLevel, className, null, msgSupplier, throwable);
    }

    public NatsLogEvent(final Level logLevel, final String className, final String message,
                        final Supplier<String> msgSupplier, final Throwable throwable) {
        this.logLevel = logLevel;
        this.className = className;
        this.message = message;
        this.throwable = throwable;
        this.msgSupplier = msgSupplier;
    }

    public Level getLogLevel() {
        return logLevel;
    }

    public String getClassName() {
        return className;
    }

    public String getMessage() {
        if (msgSupplier != null) {
            //in case provided, the supplier has precedence over a standard message.
            return msgSupplier.get();
        } else {
            return message;
        }
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public ZonedDateTime getEventTime() {
        return eventTime;
    }

    public String getFormattedEventTime() {
        return DateTimeFormatter.ISO_LOCAL_TIME.format(eventTime);
    }

    @Override
    public String toString() {
        return "NatsLogEvent{" +
                "logLevel=" + logLevel +
                ", className='" + className + '\'' +
                ", message='" + getMessage() + '\'' +
                ", throwable=" + throwable +
                ", eventTime=" + eventTime +
                '}';
    }
}
