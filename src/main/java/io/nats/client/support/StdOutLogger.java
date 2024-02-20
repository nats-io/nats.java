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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Level;

/**
 * The default nats.java logger printing messages to stdout with a prefixed timestamp.
 * Per Default the min level is set to INFO.
 */
public class StdOutLogger extends NatsLogger {

    public StdOutLogger() {
        minLevel = Level.INFO;
    }

    public StdOutLogger(final Level initialLevel) {
        minLevel = initialLevel;
    }

    @Override
    public void log(final NatsLogEvent event) {
        String logMessage = "[" + event.getFormattedEventTime() + "] " + event.getMessage();
        if (event.getThrowable() != null) {
            logMessage += ": " + stackTraceToString(event.getThrowable());
        }
        System.out.println(logMessage);
    }

    private String stackTraceToString(final Throwable throwable) {
        if (throwable == null) {
            return "";
        }
        final StringWriter sw = new StringWriter();
        throwable.printStackTrace(new PrintWriter(sw, true));
        return sw.toString();
    }
}
