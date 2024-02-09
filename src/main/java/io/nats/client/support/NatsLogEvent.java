// Copyright 2020 The NATS Authors
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

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.logging.Level;

public class NatsLogEvent {
    private static final DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendInstant(3).toFormatter();
    private final Level logLevel;

    private final String className;

    private final String message;

    private final Throwable throwable;

    private final Instant eventTime = Instant.now();

    public NatsLogEvent(Level logLevel, String className, String message) {
        this(logLevel, className, message, null);
    }

    public NatsLogEvent(Level logLevel, String className, String message, Throwable throwable) {
        this.logLevel = logLevel;
        this.className = className;
        this.message = message;
        this.throwable = throwable;
    }

    public Level getLogLevel() {
        return logLevel;
    }

    public String getClassName() {
        return className;
    }

    public String getMessage() {
        return message;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public Instant getEventTime() {
        return eventTime;
    }

    public String getFormattedEventTime() {
        return formatter.format(eventTime);
    }

    @Override
    public String toString() {
        return "NatsLogEvent{" +
                "logLevel=" + logLevel +
                ", className='" + className + '\'' +
                ", message='" + message + '\'' +
                ", throwable=" + throwable +
                ", eventTime=" + eventTime +
                '}';
    }
}
