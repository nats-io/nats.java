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

import io.nats.client.Options;

import java.util.function.Supplier;
import java.util.logging.Level;

/**
 * This is the simple logger facade that takes a logger implementation and forwards data to that logger.
 * trace log is mapped to Java FINEST level. All other log levels are mapped to their same name java logging level.
 */
public class NatsLoggerFacade {

    private static NatsLogger DEFAULT_NATS_LOGGER = new StdOutLogger(Level.OFF);
    private NatsLogger NATS_LOGGER;
    private final String CLASS_NAME;

    public NatsLoggerFacade(final String CLASS_NAME) {
        this(DEFAULT_NATS_LOGGER, CLASS_NAME);
    }

    public NatsLoggerFacade(final NatsLogger NATS_LOGGER, final String CLASS_NAME) {
        this.NATS_LOGGER = NATS_LOGGER;
        this.CLASS_NAME = CLASS_NAME;
    }

    /**
     * Obtain a new instance for logging
     * @param clazz clazz where logs originate
     * @return an instance of logger facade
     */
    public static NatsLoggerFacade getLogger(final Class<?> clazz) {
        return new NatsLoggerFacade(clazz.getName());
    }

    /**
     * Obtain a new instance for logging
     * @param clazz clazz where logs originate
     * @param natsLogger the NatsLogger used upon creation
     * @return an instance of logger facade
     */
    public static NatsLoggerFacade getLogger(final Class<?> clazz, final NatsLogger natsLogger) {
        return new NatsLoggerFacade(natsLogger, clazz.getName());
    }

    /**
     * Log Method
     * @param logLevel logging level
     * @param message the string message with any content
     */
    public void log(final Level logLevel, final String message) {
       log(logLevel, message, null);
    }

    /**
     * Log with throwable that can be written to log as printable stack trace
     * @param logLevel logging level
     * @param message the string message with any content
     * @param throwable the throwable
     */
    public void log(final Level logLevel, final String message, final Throwable throwable) {
        if (logLevel.intValue() >= NATS_LOGGER.getMinLevel().intValue()) {
            NATS_LOGGER.log(new NatsLogEvent(logLevel, CLASS_NAME, message, throwable));
        }
    }

    /**
     * Log method
     * @param logLevel logging level
     * @param msgSupplier the string message as a method reference
     */
    public void log(final Level logLevel, final Supplier<String> msgSupplier) {
        log(logLevel, msgSupplier, null);
    }


    /**
     * Log with throwable that can be written to log as printable stack trace
     * @param logLevel logging level
     * @param msgSupplier the string message as a method reference
     * @param throwable the throwable
     */
    public void log(final Level logLevel, final Supplier<String> msgSupplier, final Throwable throwable) {
        if (logLevel.intValue() >= NATS_LOGGER.getMinLevel().intValue()) {
            NATS_LOGGER.log(new NatsLogEvent(logLevel, CLASS_NAME, msgSupplier, throwable));
        }
    }

    /**
     * HelperMethod for trace logs > Converts to JAVA FINEST
     */
    public void trace(final String message) {
        log(Level.FINEST, message);
    }

    public void trace(final String message, final Throwable throwable) {
        log(Level.FINEST, message, throwable);
    }

    public void trace(final Supplier<String> msgSupplier, final Throwable throwable) {
        log(Level.FINEST, msgSupplier, throwable);
    }

    public void trace(final Supplier<String> msgSupplier) {
        trace(msgSupplier, null);
    }

    public void info(final String message) {
        log(Level.INFO, message);
    }

    public void info(final String message, final Throwable throwable) {
        log(Level.INFO, message, throwable);
    }

    public void info(final Supplier<String> msgSupplier, final Throwable throwable) {
        log(Level.INFO, msgSupplier, throwable);
    }

    public void info(final Supplier<String> msgSupplier) {
        info(msgSupplier, null);
    }

    public void warning(final String message) {
        log(Level.WARNING, message);
    }

    public void warning(final String message, final Throwable throwable) {
       log(Level.WARNING, message, throwable);
    }

    public void warning(final Supplier<String> msgSupplier, final Throwable throwable) {
        log(Level.WARNING, msgSupplier, throwable);
    }

    public void warning(final Supplier<String> msgSupplier) {
        warning(msgSupplier, null);
    }

    public void severe(final String message) {
        log(Level.SEVERE, message);
    }

    public void severe(final String message, final Throwable throwable) {
        log(Level.SEVERE, message, throwable);
    }

    public void severe(final Supplier<String> msgSupplier, final Throwable throwable) {
        log(Level.SEVERE, msgSupplier, throwable);
    }

    public void severe(final Supplier<String> msgSupplier) {
        severe(msgSupplier, null);
    }

    public void setNatsLoggerViaOptions(final Options options) {
        if (options.getNatsLogger().getClass() != StdOutLogger.class && options.getNatsLogger().getMinLevel() != Level.OFF) {
            setNatsLogger(options.getNatsLogger());
        } else if (options.isTraceConnection()) {
            setNatsLogger(new StdOutLogger());
        }
    }

    public void setNatsLogger(final NatsLogger natsLogger) {
        if (natsLogger != null) {
            NATS_LOGGER = natsLogger;
        }
    }

    public NatsLogger getNatsLogger() {
        return NATS_LOGGER;
    }

    public static void setDefaultNatsLogger(final NatsLogger defaultNatsLogger) {
        if (defaultNatsLogger != null) {
            DEFAULT_NATS_LOGGER = defaultNatsLogger;
        }
    }
}
